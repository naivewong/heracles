import sys

if __name__ == "__main__":
	with open(sys.argv[1]) as f1, open(sys.argv[2]) as f2, open(sys.argv[3]) as f3:
		line1 = f1.readline()
		line2 = f2.readline()
		line3 = f3.readline()

		while line1:
			if not line1.startswith("-"):
				print(line1.strip())
				line1 = f1.readline()
				line2 = f2.readline()
				line3 = f3.readline()
				continue
			s = float(line1.split("=")[-1]) + float(line2.split("=")[-1]) + float(line3.split("=")[-1])
			s /= float(3)
			print(line1.split("=")[0] + "=" + str(s))
			line1 = f1.readline()
			line2 = f2.readline()
			line3 = f3.readline()