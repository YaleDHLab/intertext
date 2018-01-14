# Intertext
> Detect and visualize text reuse.

Intertext combines machine learning with interactive data visualizations to surface intertextual patterns in large text collections. The text processing all happens in Python--using a combination of OCR-proof character vectors and minhashing--and the client-side web interactions all happen in React.

![App preview](/src/assets/images/preview.png?raw=true)

## Dependencies

This application depends upon MongoDB, Node.js, libxml2, Python2.7, and the Python package manager, pip. It's recommended to use Anaconda to manage the Python dependencies.

If you use OSX, you can install most of these dependencies with Homebrew:

```
brew install libxml2
brew link --force libxml2
brew install node
brew install mongodb
brew services start mongodb
```

## Quickstart

To see the application in action, you can run:

```
npm i
conda create --name 2.7 python=2.7
source activate 2.7
npm run install-python-dependencies
npm run detect-reuse
npm run production
```

Then open a web server on `localhost:8080`.

## Processing New Data

To process new data, you first need to install the Python requirements identified in `/utils/requirements.txt`. You may want to do so within a virtual environment or a Conda environment.

The application includes a helper function that will attempt to install these modules for you automatically:

```
npm run install-python-dependencies
```

After installing the dependencies, add your data to `/utils/data`, then update `utils/config.json` so as to point the config file to your data and metadata. Make sure your new data and metadata are in the same format as the sample data included in `utils/data/camus`.

Once your data is in place, you can identify intertextualities in the data by running:

```
npm run production
npm run detect-reuse
```

After processing your texts, you can repopulate the database table used by the application by running:

```
npm run seed
```

## Configuration

The following values within `/utils/config.json` control the way this application attempts to detect instances of text reuse:

| Field  | Remarks |
| ------------- | ------------- |
| path_to_corpus | A glob path to the files to be scoured for text reuse |
| path_to_metadata | Metadata describing each file to be processed |
| build_minhashes | `{0,1}` Controls whether new minhashes are generated |
| build_matches | `{0,1}` Controls whether new matches are identified and stored |
| max_processes | The maximum number of cpu cores to use |
| max_cached_records | Increasing this value speeds up processing but requires more RAM |
| remove_punctuation | `{0,1}` indicates whether to strip punctuation for analysis/display |
| remove_diacritics | `{0,1}` indicates whether to strip diacritics for analysis |
| log_frequency | Increasing this value decreases the frequency the app logs its progress |
| strip_xml_tags | `{0,1}` indicates whether the files to process need XML stripping |
| xml_text_node | The XML node from which to extract text (if applicable) |
| window_length | Words in each 'window'. Increasing this makes matches longer |
| step_size | Words to skip when sliding each window |
| *min_similarity | Increasing this raises precision but lowers recall |
| *permutations | Increasing this raises precision but lowers recall |
| *recall | Increasing this raises recall but slows processing |
\* = essential analytic parameter

## Linking to other domains

Each text can be given a `url` attribute in the corpus metadata file. If a file is given a url from another domain, users will be able to click on the Read icon under each match to read the given file on the specified domain.

If your texts are paginated on the remote domain and your input texts are in well-structured XML format, you can use the `paginated_deeplinks` attribute in the corpus metadata to specify the logic the application should use to try and create a mapping from each segment of your file to the page number for that segment. The `tag` attribute within `paginated_deeplinks` should specify the tag that delimits page breaks in your documents, while the `attribute` should specify the attribute within those tags that indicates page number. For example, suppose your XML documents denoted page breaks with the following tags:

```
<pb n="7" />
```

To create paginated deeplinks for this kind of tag, you could use the following values within your corpus metadata file:

```
"paginated_deeplinks": {
  "generate": 1,
  "tag": "pb",
  "attribute": "n"
}
```

Please note that the creation of paginated deeplinks requires well-structured XML in which page tags are self-closing.

If you are using paginated deeplinks and want to add a suffix to the url for a given file, you can specify a `url_suffix` attribute in the metadata for the given file, e.g.:

```
"34360.txt": {
  "author": "Thomas Gray",
  "url": "http://spenserians.cath.vt.edu/TextRecord.php?action=GET&textsid=34360",
  "image": "/assets/image/author_images/thomas-gray.jpg",
  "title": "An Elegy wrote in a Country Churchyard.",
  "module": "plagiary_poets",
  "publication_year": "1751",
  "url_suffix": "&lang=en"
}
```

## Deploying on AWS

The following covers steps you can take to deploy this application on an Amazon Linux AMI on AWS. After creating and ssh-ing to the instance, run:

```
sudo yum groupinstall "Development Tools" -y

##
# Python dependencies
##

sudo yum install libxml2-devel libxslt-devel python-devel
wget https://repo.continuum.io/archive/Anaconda2-4.1.1-Linux-x86_64.sh
bash Anaconda2-4.1.1-Linux-x86_64.sh

# accept the license agreement
source ~/.bashrc
which conda

# create a virtual environment for your Python dependencies
conda create --name 2.7 python=2.7
source activate 2.7
npm run install-python-dependencies

##
# Node
##

curl -o- https://raw.githubusercontent.com/creationix/nvm/v0.32.0/install.sh | bash
. ~/.nvm/nvm.sh
nvm install 6.10.0
node -v

##
# Mongo
##

sudo touch /etc/yum.repos.d/mongodb-org-3.4.repo
sudo vim /etc/yum.repos.d/mongodb-org-3.4.repo

# paste the following:
[mongodb-org-3.4]
name=MongoDB Repository
baseurl=https://repo.mongodb.org/yum/amazon/2013.03/mongodb-org/3.4/x86_64/
gpgcheck=1
enabled=1
gpgkey=https://www.mongodb.org/static/pgp/server-3.4.asc

sudo yum install -y mongodb-org
sudo service mongod start
sudo chkconfig mongod on
```

Then clone this repo and follow the installation instructions above. After doing so, you should be able to see the application at `http://YOUR_EC2_INSTANCE_IP:8080`. To make the service run on a different port, you can update `config.json`.
