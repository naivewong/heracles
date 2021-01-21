import sys

if __name__ == "__main__":
	lines = []
	duration = {}
	samples = {}
	with open(sys.argv[1]) as f1, open(sys.argv[2]) as f2, open(sys.argv[3]) as f3:
		line = f1.readline()
		line1 = f2.readline()
		line2 = f3.readline()
		idx = 0
		while line:
			if line.startswith("> complete stage"):
				lines.append("".join(line.strip().split("=")[:-1]) + "=")
				d = float(line.strip().split("=")[-1])
				duration[idx] = d
				duration[idx] += float(line1.strip().split("=")[-1])
				duration[idx] += float(line2.strip().split("=")[-1])
				duration[idx] /= float(3)
			elif line.startswith("  > total samples"):
				lines.append(line.strip().split("=")[0] + "=")
				s = int(line.strip().split("=")[-1])
				samples[idx] = s
				samples[idx] += int(line1.strip().split("=")[-1])
				samples[idx] += int(line2.strip().split("=")[-1])
				samples[idx] /= int(3)
			else:
				lines.append(line.strip())
			idx += 1
			line = f1.readline()
			line1 = f2.readline()
			line2 = f3.readline()
	for i in range(len(lines)):
		if lines[i].startswith("> complete stage"):
			print(lines[i]+str(duration[i]))
		elif lines[i].startswith("> total samples"):
			print(lines[i]+str(samples[i]))
		else:
			print(lines[i])