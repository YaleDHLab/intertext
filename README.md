# Intertext
> Detect and visualize text reuse.

Intertext combines machine learning with interactive data visualizations to surface intertextual patterns in large text collections. The text processing is based on minhashing vectorized strings, and the web viewer is based on interactive javascript components.

![App preview](/src/assets/images/preview.png?raw=true)

## Dependencies

This application uses Redis as a chache and MongoDB as a database. You can install and start them on OSX with the following:

```
brew install redis mongodb
brew services start redis
brew services start mongodb
```

For data processing, this app uses Python 3.5 and the Python package manager pip. We highly recommend installing Anaconda, starting a conda environment, then installing the Python dependencies:

```
pip install -r intertext/requirements.txt --user
```

Finally, this app uses Node.js as a web server. You can install Node on OSX with the following command:

```
brew install node
```

## Quickstart

Once the dependencies outlined above are installed, you can run:

```
# clone the application source code
git clone https://github.com/YaleDHLab/intertext

# install the node dependencies
cd intertext && npm install

# detect text reuse in the included sample documents
npm run detect-reuse

# start the web server
npm run production
```

If you open a web browser to `localhost:7092`, you will be able to browse discovered intertexts.

## Processing New Data

To process new data, you need to install the app dependencies, then replace the files in `data/texts` with your text files and replace the metadata file in `data/metadata` with a new metadata file. Make sure your new text files and metadata files are in the same format as the sample text and metadata files.

Once your files are in place, you can identify intertexts in the data by running:

```
npm run detect-reuse
```

After processing your texts, you can examine the discovered text reuse by running:

```
npm run production
```

Then navigate to `localhost:7092` and search for an author or text of interest.

## config.json

The following values within `config.json` control the way Intertext discovers text reuse:

| Field  | Remarks |
| ------------- | ------------- |
| infiles | A glob path to the files to be searched for text reuse |
| metadata | A path to the metadata file describing each input file |
| xml_tag | The XML node from which to extract input text (if applicable) |
| max_cores | The maximum number of cpu cores to use during processing |
| step | Words to skip when sliding each window |
| window_size | Increasing this lowers recall but finds more significant matches |
| *n_permutations | Increasing this raises recall but lowers speed |
| *hashband_length | Increasing this lowers recall but raises speed |
| *min_similarity | Increasing this raises precision but lowers recall |
\* = *essential analytic parameter*

**Sample config.json file**:

```
{
  "infiles": "data/texts/*.txt",
  "metadata": "data/metadata/metadata.json",
  "xml_tag": false,
  "max_cores": 8,
  "step": 4,
  "window_size": 14,
  "n_permutations": 256,
  "hashband_length": 3,
  "min_similarity": 0.65
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

## Configuring RAM Usage

To set an upper-bound to the amount of RAM Redis will use during data processing, you can run `redis-cli` from a terminal to enter the Redis shell, then type:

```
CONFIG SET maxmemory {{ maximum_memory_in_bytes }}
```

For example, to set the maximum memory to 2GB, you can run:

```
CONFIG SET maxmemory 2000000000
```

Please be aware that the Redis will sometimes use more than the maximum specified RAM allocation. This happens for two reasons: the first is that Redis checks RAM usage only periodically, so usage may climb above the upper bound between RAM checks, and the second is that the Redis process "forks" itself periodically to transfer data from RAM to disk. During each fork, the amount of RAM Redis uses doubles, so if one sets an upper bound of 2GB for a Redis process, Redis may require roughly 6GB of RAM during periods of data processing.

When in doubt, set a conservative upper bound to the maximum RAM allocation.

## Deploying on AWS

The following covers steps you can take to deploy this application on an Amazon Linux AMI on AWS.

While creating the instance, add the following Custom TCP Ports to the default security settings:

| Port Range  | Source | Description |
| ----------- | ------ | ----------- |
| 80 | 0.0.0.0/0, ::/0 | HTTP |
| 443 | 0.0.0.0/0, ::/0 | HTTPS |
| 6379 | 0.0.0.0/0, ::/0 | Redis |
| 27017 | 0.0.0.0/0, ::/0 | MongoDB |

After creating and ssh-ing to the instance, you can install all application dependencies, process the sample data, and start the web server with the following commands.

```
sudo yum update -y
sudo yum groupinstall "Development Tools" -y

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

##
# Redis
##

# install Redis
wget https://gist.githubusercontent.com/duhaime/c401e86d2c4f89cf079044e1474f8f84/raw/41f4d4ed66409fcd30f2fd9a1a5b270687798e5a/install_redis.sh
bash install_redis.sh
rm install_redis.sh

# restart the Redis server
sudo service redis-server restart

##
# Python dependencies
##

sudo yum install libxml2-devel libxslt-devel python-devel -y
wget https://repo.continuum.io/archive/Anaconda2-4.1.1-Linux-x86_64.sh
bash Anaconda2-4.1.1-Linux-x86_64.sh

# accept the license agreement and default install location
source ~/.bashrc
which conda
rm Anaconda2-4.1.1-Linux-x86_64.sh

# create a virtual environment for your Python dependencies
conda create --name 3.5 python=3.5
source activate 3.5

# obtain app source and install Python dependencies
git clone https://github.com/YaleDHLab/intertext
cd intertext
pip install -r intertext/requirements.txt --user

##
# Intertext
##

# install node dependencies
npm install

# process texts
npm run detect-reuse

# start the server
npm run production
```

After running these steps (phew!), you should be able to see the application at http://YOUR_INSTANCE_IP:7092. To make the service run on a different port, specify a different port in `server/config.json`.

To forward requests for http://YOUR_INSTANCE_IP to port 7092, run:

```
sudo iptables -t nat -A PREROUTING -p tcp --dport 80 -j REDIRECT --to-ports 7092
```

Then users can see your application at http://YOUR_INSTANCE_IP without having to state a port.