# Orias Summary model trainer

## Vietnamese News Summarization Model

### Introduction

This project implements a complete pipeline to automatically crawl Vietnamese news articles from VnExpress and subsequently train a Transformer model for abstractive text summarization.

### Key Features

* **Intelligent Data Crawling** : Automatically scrapes data from specified categories on VnExpress.
* **Hybrid Crawling Pipeline** : Utilizes a sequential "scan" to filter out low-quality sub-categories, then accelerates data collection using a multi-threaded approach.
* **Flexible Configuration** : The entire workflow, from crawling to training, is controlled via a single Jupyter Notebook, allowing for easy parameter changes without modifying the core source code.
* **Transformer Fine-Tuning** : Fine-tunes the `vinai/bartpho-syllable` model, a large language model for Vietnamese, to specialize it for the task of news summarization.
* **Reproducible Environment** : Provides an `environment.yml` file to accurately recreate the working environment and avoid library conflicts.

### Technology Stack

* **Language** : Python 3.11
* **Environment** : Conda
* **Data Crawling** : `Requests`, `BeautifulSoup4`
* **Data Processing** : `Pandas`
* **AI/ML Platform** : `PyTorch`
* **Models & Training** : `Hugging Face Transformers`, `Datasets`, `Accelerate`, `Evaluate`
* **Control Interface** : `Jupyter Notebook`

### Project Structure

```
SUMMARY AI
│
├─ database/			# Directory for storing the crawled data.
│  ├─ JSON/
│  └─ XLSX/
│
├─ Libraries/			# Directory of defined modules.
│  ├─ Crawler.py		# The data crawling engine
│  └─ Trainer.Py		# The model training engine
│
├─ Models/			# Directory for storing the fine-tuned models.
│
├─ .gitignore
├─ env.yml			# Defines the Conda environment.
├─ Main.ipynb			# The main "control panel" for running the entire pipeline.
└─ README.md
```

## Setup and Installation

#### Stage 1: Data Collection

In the notebook, you will find a cell for configuring the crawler. You can customize the parameters as needed. Running these cells will initiate the data collection and processing. The data will be saved in the `Database` directory.

#### Stage 2: Model Training

Similarly, you can customize the hyperparameters for the training process. Running these cells will start the fine-tuning process. The trained model will be saved in the `models` directory.

#### Stage 3: Using the Model for Summarization

The notebook also provides a final cell that allows you to load your newly trained model and test it by summarizing any given text.

#### Setup step:

**RECOMENCE: use ANACONDA or MINICONDA**

* conda install cmd:
  * curl -LO https://repo.anaconda.com/miniconda/Miniconda3-latest-Windows-x86_64.exe
* conda version check:
  * conda --version

- Clone this Repository
- Install necessary extension for Python and Jupyter (VS Code)
- Install necessary module in virtual envionment:
  - cmd: conda env create -f env.yml	*# This virtual environment will be "master"*
- Open Main.ipynb and enjoy

---

Why didn't I write this README by Vietnamese?

I like to do that =))

---
