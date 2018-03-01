# Intertext
> Detect and visualize text reuse within collections of plain text or XML documents.

Intertext combines machine learning with interactive data visualizations to surface intertextual patterns in large text collections. The text processing is based on minhashing vectorized strings, and the web viewer is based on interactive React components.

![App preview](/src/assets/images/preview.png?raw=true)

## Dependencies

This application uses MongoDB as a database. You can install and start MongoDB on OSX with the following:

```
brew install mongodb
brew services start mongodb
```

This app also uses Node.js as a web server. You can install Node on OSX with the following command:

```
brew install node
```

## Quickstart

Once the dependencies outlined above are installed, you can run:

```
# clone the application source code
git clone https://github.com/YaleDHLab/intertext

# install the Python dependencies
cd intertext && pip install -r requirements.txt --user

# install the node dependencies
npm install --no-optional

# detect reuse in the included sample documents
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

| Field  | Default | Remarks |
| ------------- | ------ | ------------- |
| infiles | None | Glob path to files to be searched for text reuse |
| metadata | None | Path to the metadata file describing each input file |
| max_cores | 8 | Maximum number of cpu cores to use during processing |
| window_size | 14 | Words in each window. Increase to find longer matches |
| step | 4 | Words to skip when sliding each window |
| xml_tag | False | XML node from which to extract input text (if relevant) |
| same_author_matches | True | Store matches where source author == target author ? |
| mongo_host | localhost | The host on which Mongo is running |
| mongo_port | 27017 | The port on which Mongo is running |
| *n_permutations | 256 |  Increasing this raises recall but lowers speed |
| *hashband_length | 4 | Increasing this lowers recall but raises speed |
| *min_similarity | 0.65 | Increasing this raises precision but lowers recall |
\* = *essential analytic parameter*

Providing a value for one of the files above will override the default value.

**Sample config.json file**:

```
{
  "infiles": "data/texts/*.txt",
  "metadata": "data/metadata/metadata.json",
  "max_cores": 7,
  "min_similarity": 0.75
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

## Running on a Compute Cluster

If you have access to a multi-node compute cluster (a.k.a. a supercomputer), you can run intertext jobs by creating a number of jobs and passing two integer arguments to each intertext process on the command line. The first of these arguments should identify the index value of the given job, and the second should identify the total number of jobs that will run. For example, to run 75 jobs on a Sun Grid Engine queueing system that uses `module` as a dependency manager, one can submit the following job task:

```bash
#!/bin/bash
#$ -N job-name
#$ -o output.log
#$ -t 1-75:1
#$ -r y
source ~/.bash_profile
module load python/3.6.0
python3 intertext/minhash.py ${SGE_TASK_ID} 75
```

This can be submitted with `qsub FILENAME.sh` where FILENAME refers to the name of the bash file with the content above. Each of those intertext processes will receive a unique job id as `sys.argv[1]` and the total number of jobs as `sys.argv[2]`.

Please note all jobs will need to finish a task before any job moves on, so you should only submit a number of jobs equal to the number you can expect to run at the same time on the compute cluster.

## Deploying on AWS

The following covers steps you can take to deploy this application on an Amazon Linux AMI on AWS.

While creating the instance, add the following Custom TCP Ports to the default security settings:

| Port Range  | Source | Description |
| ----------- | ------ | ----------- |
| 80 | 0.0.0.0/0, ::/0 | HTTP |
| 443 | 0.0.0.0/0, ::/0 | HTTPS |
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
pip install -r requirements.txt --user

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