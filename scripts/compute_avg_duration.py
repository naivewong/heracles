import sys

if __name__ == "__main__":
    lines = sys.argv[1].split("\n")
    sum = 0.0
    for line in lines:
        line = line.split("=")[1]
        if line.endswith("ms"):
            sum += float(line[:-2])/1000
        else:
            sum += float(line[:-1])
    print("lines", len(lines))
    print(sum/float(len(lines)))
