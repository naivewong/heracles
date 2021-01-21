import re

f2 = open("../testdata/devops_series_big.json", "w")
with open("../testdata/devops_series.json", "r") as f1:
	line = f1.readline().strip()
	while line:
		f2.write(line+"\n")
		line = f1.readline().strip()

for i in range(10)[1:]:
	with open("../testdata/devops_series.json", "r") as f1:
		line = f1.readline().strip()
		while line:
			x = re.search("host_[0-9]*", line)
			newHost = line[x.start(): x.end()].split("_")[0] + "_" + str(int(line[x.start(): x.end()].split("_")[1]) + i * 1000)
			f2.write(line[:x.start()] + newHost + line[x.end():] + "\n")
			line = f1.readline().strip()
f2.close()