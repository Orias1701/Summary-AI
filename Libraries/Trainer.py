# Tên file: trainer_module.py

import pandas as pd
import json
import numpy as np
import evaluate
from datasets import Dataset, DatasetDict
from transformers import (
    AutoTokenizer,
    AutoModelForSeq2SeqLM,
    DataCollatorForSeq2Seq,
    Seq2SeqTrainingArguments,
    Seq2SeqTrainer,
)

class SummarizationTrainer:
    """
    Một lớp để fine-tune mô hình tóm tắt Transformer với cấu hình tùy chỉnh.
    """
    def __init__(self, config):
        """
        Khởi tạo trainer với một dictionary cấu hình.
        """
        self.config = config
        self.data_jsonl_file = config.get("DATA_JSONL_FILE")
        self.model_checkpoint = config.get("MODEL_CHECKPOINT", "vinai/bartpho-syllable")
        self.output_model_dir = config.get("OUTPUT_MODEL_DIR", "../Models/bartpho-summarizer-v1")
        self.max_input_length = config.get("MAX_INPUT_LENGTH", 1024)
        self.max_target_length = config.get("MAX_TARGET_LENGTH", 256)
        self.batch_size = config.get("BATCH_SIZE", 4)
        self.num_train_epochs = config.get("NUM_TRAIN_EPOCHS", 3)
        self.learning_rate = config.get("LEARNING_RATE", 2e-5)
        self.weight_decay = config.get("WEIGHT_DECAY", 0.01)

        # Tải tokenizer một lần và tái sử dụng
        self.tokenizer = AutoTokenizer.from_pretrained(self.model_checkpoint)

    def _load_and_prepare_data(self):
        """Đọc file jsonl an toàn, chuyển thành Dataset và chia tập train/validation."""
        print(f"Đang tải dữ liệu từ {self.data_jsonl_file}...")
        
        data_list = []
        with open(self.data_jsonl_file, 'r', encoding='utf-8') as f:
            for line in f:
                if not line.strip(): continue
                try:
                    data_list.append(json.loads(line))
                except json.JSONDecodeError:
                    print(f"Cảnh báo: Bỏ qua một dòng JSON không hợp lệ.")
                    continue
        
        df = pd.DataFrame(data_list)
        df = df[["content", "description"]].rename(columns={"content": "text", "description": "summary"}).dropna()

        print(f"Tổng cộng có {len(df)} mẫu dữ liệu hợp lệ.")
        
        dataset = Dataset.from_pandas(df)
        train_test_split = dataset.train_test_split(test_size=0.1)
        
        dataset_dict = DatasetDict({'train': train_test_split['train'], 'validation': train_test_split['test']})
        
        print(f"Dữ liệu đã chia: {len(dataset_dict['train'])} train, {len(dataset_dict['validation'])} validation.")
        return dataset_dict

    def _preprocess_function(self, examples):
        """Hàm tiền xử lý, chuyển văn bản thành các ID mà model hiểu được."""
        inputs = self.tokenizer(examples["text"], max_length=self.max_input_length, truncation=True)
        with self.tokenizer.as_target_tokenizer():
            labels = self.tokenizer(examples["summary"], max_length=self.max_target_length, truncation=True)
        inputs["labels"] = labels["input_ids"]
        return inputs

    def _compute_metrics(self, eval_pred):
        """Tính toán điểm ROUGE trong quá trình validation."""
        predictions, labels = eval_pred
        decoded_preds = self.tokenizer.batch_decode(predictions, skip_special_tokens=True)
        labels = np.where(labels != -100, labels, self.tokenizer.pad_token_id)
        decoded_labels = self.tokenizer.batch_decode(labels, skip_special_tokens=True)
        
        rouge_metric = evaluate.load("rouge")
        result = rouge_metric.compute(predictions=decoded_preds, references=decoded_labels, use_stemmer=True)
        result = {key: value * 100 for key, value in result.items()}
        
        prediction_lens = [np.count_nonzero(pred != self.tokenizer.pad_token_id) for pred in predictions]
        result["gen_len"] = np.mean(prediction_lens)
        
        return {k: round(v, 4) for k, v in result.items()}

    def run(self):
        """
        Thực thi toàn bộ quy trình fine-tuning.
        """
        # 1. Tải và chuẩn bị dữ liệu
        raw_datasets = self._load_and_prepare_data()
        
        # 2. Tokenize dữ liệu
        print("\nBắt đầu tokenize dữ liệu...")
        tokenized_datasets = raw_datasets.map(self._preprocess_function, batched=True)
        
        # 3. Tải model nền tảng
        print(f"\nTải model nền tảng: {self.model_checkpoint}...")
        model = AutoModelForSeq2SeqLM.from_pretrained(self.model_checkpoint)
        
        # 4. Cấu hình các tham số huấn luyện
        training_args = Seq2SeqTrainingArguments(
            output_dir=self.output_model_dir,
            eval_strategy="epoch", # Đổi lại thành eval_strategy nếu thư viện cũ
            learning_rate=self.learning_rate,
            per_device_train_batch_size=self.batch_size,
            per_device_eval_batch_size=self.batch_size,
            weight_decay=self.weight_decay,
            save_total_limit=3,
            num_train_epochs=self.num_train_epochs,
            predict_with_generate=True,
            fp16=True,
            push_to_hub=False,
        )

        data_collator = DataCollatorForSeq2Seq(tokenizer=self.tokenizer, model=model)
        
        trainer = Seq2SeqTrainer(
            model=model,
            args=training_args,
            train_dataset=tokenized_datasets["train"],
            eval_dataset=tokenized_datasets["validation"],
            tokenizer=self.tokenizer,
            data_collator=data_collator,
            compute_metrics=self._compute_metrics,
        )
        
        # 5. Bắt đầu huấn luyện
        print("\n--- BẮT ĐẦU HUẤN LUYỆN ---")
        trainer.train()
        print("--- HUẤN LUYỆN HOÀN TẤT ---")
        
        # Lưu model cuối cùng
        trainer.save_model(self.output_model_dir)
        print(f"Model đã được lưu tại: {self.output_model_dir}")