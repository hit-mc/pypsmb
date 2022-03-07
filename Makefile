files: pypsmb main.py config.yml Dockerfile requirements.txt

release: files
	tar czvf release.tar.gz --exclude='__pycache__' pypsmb main.py config.yml Dockerfile requirements.txt

start-docker: files
	docker rm -f pypsmb
	docker build -t pypsmb .
	docker run -p13880:13880 --name pypsmb -it pypsmb