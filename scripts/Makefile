translations:
	poetry run python generate.py cli-generate

leaderboard: translations
	poetry run python evaluate_results.py build-supervised-leaderboard
