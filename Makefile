default:
	python3 main.py

init:
	sh ./init.sh

clean:
	[ -e metadata.parquet ] && rm metadata.parquet
