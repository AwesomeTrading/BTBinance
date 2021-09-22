env:
	conda config --append channels conda-forge || true
	conda create -y --prefix ./.venv python=3.8
	# eval "$(conda shell.bash hook)"
	# conda activate ./.venv
	# pip install -r requirements.txt