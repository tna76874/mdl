# Mediathek Downloader

#### Manual installation

requirements:

```bash
sudo apt-get install libatlas-base-dev
```

Installation

```bash
pip3 install --upgrade git+https://github.com/tna76874/mdl.git
```

Local:

```bash
git clone https://github.com/tna76874/mdl.git
cd mdl
pip3 install --upgrade .
```

#### Docker

```bash
./build.sh

./run.sh
```

or

```bash
docker run --rm -v $PWD/download:/download -v $PWD/config:/config ghcr.io/tna76874/mdl:latest --version
```

