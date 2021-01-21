import sys

if __name__ == "__main__":
	for line in sys.argv[1].split("\n"):
		if line.startswith("BenchmarkDBtsbs/BenchmarkDB"):
			print(line.replace("BenchmarkDBtsbs/BenchmarkDB", "Benchmark"))
		elif line.startswith("BenchmarkGroupDBtsbs/BenchmarkGroupDB"):
			print(line.replace("BenchmarkGroupDBtsbs/BenchmarkGroupDB", "Benchmark"))
		else:
			print(line)