release: mb main.py config.yml Dockerfile requirements.txt
	tar czvf release.tar.gz mb main.py config.yml Dockerfile requirements.txt