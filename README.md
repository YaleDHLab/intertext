# Installation

```bash
# clone the source
git clone https://github.com/yaledhlab/intertext

# move into the intertext directory
cd intertext

# install the module
python setup.py install
```

# Usage

```bash
# create output
intertext --infiles "sample_data/texts/*.txt" --metadata "sample_data/metadata.json" --update_client

# serve output
cd output && python -m http.server
```