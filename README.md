# Intertext
> Detect and visualize text reuse within collections of plain text or XML documents.

Intertext combines machine learning with interactive data visualizations to surface intertextual patterns in large text collections. The text processing is based on minhashing vectorized strings, and the web viewer is based on interactive React components.

![App preview](/src/assets/images/preview.png?raw=true)

## Quickstart

This application uses MongoDB as a database, Redis as a cache for message passing, and Node.js as a Web server. You can install these dependencies and start the services on OSX with the following:

```
brew install mongodb redis node
brew services start mongodb
brew services start redis
```

Once those dependencies are installed, you can install the Python and JavaScript dependencies by running:

```
# clone the application source code
git clone https://github.com/YaleDHLab/intertext

# install the Python dependencies
cd intertext
pip install -r requirements.txt --user

# install the node dependencies
npm install --no-optional
```

Once the dependencies are all set, you can detect text reuse in the included [sample documents](data/texts) by running:

```
# detect reuse in the included sample documents
python intertext/run.py

# start the web server
npm run production
```

If you open a web browser to `localhost:7092`, you will be able to browse discovered intertexts.

## Method

The Python resources in this repository use the following pipeline to process input documents given a [config file](#configjson).

Each input document is read in, and XML tags are stripped if the `config.xml_tag` parameter is set. We next pass a sliding window over the text, selecting `config.window_size` words for each window, and sliding the window forward `config.step` words each step. For each window, we identify each three-character sequence in the window (e.g. "quick fox" becomes: 'qui', 'uic', 'ick', 'ck ', 'k f', ' fo', 'fox').

Given the set of these three-character grams, we generate `config.n_permutations` [minhashes](https://en.wikipedia.org/wiki/MinHash) for each text window, where each minhash is an integer that gives a representation of the character trigrams for the window. We then combine each sequence of `config.hashband_length` minhashes for the current window into a "hashband". Any two text windows that share a hashband are identified as potential matches. We use a fuzzy string similarity measurement to validate each potential match, cluster adjacent matches, and format the matches into a JSON packet that can be visualized with the included web client.

A brute force approach to detecting all instances of text reuse in a corpus is essentially O(n^2), as it requires us to compare each document against each document. However, the pipeline outlined above allows us to perform this analysis in linear time, as we only need to pass over the corpus once to extract the hashbands for each window and again to identify the set of windows that share a given hashband. The character-hashing method described above is also fuzzy enough to catch text reuse in cases where word order has changed significantly and cases where spelling oddities and OCR errors create character-level errors in the input documents.

## Distributing over Multiple Servers

The Python resources in this repository use [Celery](http://www.celeryproject.org/) to distribute tasks to multiple processors on one or more servers and Redis to minimize IO. To distribute the work load over several servers (e.g. in a supercomputing context), specify a `redis_url` to which each server has access in the [config file](#configjson), then start a worker process on each of those servers by running the following from the directory that contains this README file:

```bash
# start the worker deamon
celery worker --app intertext.tasks --loglevel info
```

Once your workers are running, start the task scheduler to begin processing your data:

```bash
# process the infiles
python intertext/run.py
```

For a visual overview of the currently scheduled tasks and the workers' progress on each, you can use [flower](http://flower.readthedocs.io/en/latest/), the Celery monitoring tool, with the following command:

```
# create a progress dashboard (view on localhost:5555)
celery flower --app intertext.tasks --address=127.0.0.1 --port=5555
```

If you visit `localhost:5555` after running that command, you'll see an overview of each task in the Celery queue.

## config.json

`config.json` controls the way Intertext discovers text reuse in your corpus. The only required fields are `infiles` and `metadata`, though several other options may be specified to override the defaults:

| Field  | Default | Remarks |
| ------------- | ------ | ------------- |
| infiles | None | Glob path to files to be searched for text reuse |
| metadata | None | Path to the metadata file describing each input file |
| xml_tag | False | XML node from which to extract input text (if relevant) |
| encoding | utf8 | The encoding of the input documents |
| window_size | 14 | Words in each window. Increase to find longer matches |
| step | 4 | Words to skip when sliding each window |
| mongo_url | mongodb://localhost:27017/intertext | A valid MongoDB URI |
| redis_url | redis://localhost:6379/0 | A valid Redis URI |
| *n_permutations | 256 |  Increasing this raises recall but lowers speed |
| *hashband_length | 4 | Increasing this lowers recall but raises speed |
| *min_similarity | 0.65 | Increasing this raises precision but lowers recall |
\* = *essential analytic parameter*

Providing a value for one of the files above will override the default value.

**Minimal config.json file**:

```
{
  "infiles": "data/texts/*.txt",
  "metadata": "data/metadata/metadata.json",
}
```

## metadata.json

Each corpus must also have a `metadata.json` file that details metadata for each input file. Each input file should have one top-level key in the metadata file, and each of those keys can have any or all of the following optional attributes (example below):

| Field  | Remarks |
| ------------- | ------------- |
| author | Author of the text |
| title | Title of the text |
| year | Year in which text was published |
| url | Deeplink to a remote server with the text (or related materials) |
| image | Image of the author in `src/assets/images/authors` or on remote server |

All metadata fields are optional, though all are expressed somewhere in the browser interface.

**Sample metadata.json file**
```
{
  "34360.txt": {
    "author": "Thomas Gray",
    "title": "An Elegy wrote in a Country Churchyard.",
    "year": 1751,
    "url": "http://spenserians.cath.vt.edu/TextRecord.php?action=GET&textsid=34360",
    "image": "http://www.poemofquotes.com/thomasgray/thomas-gray.jpg"
  },
  "37519.txt": {
    "author": "Anonymous",
    "title": "Elegy written in Saint Bride's Church-Yard.",
    "year": 1769,
    "url": "http://spenserians.cath.vt.edu/TextRecord.php?action=GET&textsid=37519",
    "image": "src/assets/images/authors/default-headshot.jpg"
  }
}
```