# Intertext

> Detect and visualize text reuse within collections of plain text or XML documents.

Intertext uses machine learning and interactive visualizations to identify and display intertextual patterns in text collections. The text processing is based on minhashing vectorized strings and the web viewer is based on interactive React components. [[Demo](https://lab-apps.s3-us-west-2.amazonaws.com/intertext/spenserians-sample/index.html)]

![App preview](/docs/preview.png?raw=true)

# Installation

To install Intertext, run the steps below:

```bash
# clone the source
git clone https://github.com/YaleDHLab/intertext

# move into the intertext directory
cd intertext

# install the module
python setup.py install
```

# Usage

```bash
# search for intertextuality in some documents
intertext --infiles "sample_data/texts/*.txt"

# serve output
python -m http.server 8000
```

Then open a web browser to `http://localhost:8000` and you'll see any intertextualities the engine discovered!

## CUDA Acceleration

To enable Cuda acceleration, we recommend using the following steps when installing the module:

```bash
# set up conda virtual environment
conda create --name intertext python=3.7
conda activate intertext

# set up cuda and cupy
conda install cudatoolkit
conda install -c conda-forge cupy
```