# Installation

```bash
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
  --update_client

# serve output
cd output && python -m http.server
```