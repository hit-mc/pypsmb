release: pypsmb main.py config.yml Dockerfile requirements.txt
	tar czvf release.tar.gz pypsmb main.py config.yml Dockerfile requirements.txt