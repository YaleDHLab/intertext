# Installation

```bash

# optional: set up conda virtual environment
conda create --name intertext python=3.7
conda activate intertext

# optional: set up cuda and cupy:
conda install cudatoolkit
conda install -c conda-forge cupy

# clone the source
git clone https://github.com/YaleDHLab/intertext-module

# move into the intertext directory
cd intertext-module

# install the module
python setup.py install
```

# Usage

```bash
# search for intertextuality in some documents
intertext \
  --infiles "sample_data/texts/*.txt" \
  --metadata "sample_data/metadata.json" \
  --update_client \
  --verbose

# serve output
cd output && python -m http.server
```
