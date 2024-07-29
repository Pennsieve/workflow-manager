#!/usr/bin/python3

import shutil
import sys
import time

# Gather our code in a main() function
def main():
    print("cleaning up ...")
    time.sleep(10)
    inputDir = sys.argv[2]
    outputDir = sys.argv[3]

    try:
        shutil.rmtree(inputDir)
        print("Success: %s deleted." % (inputDir))
    except OSError as e:
        print("Error: %s - %s." % (e.filename, e.strerror))

    try:
        shutil.rmtree(outputDir)
        print("Success: %s deleted." % (outputDir))
    except OSError as e:
        print("Error: %s - %s." % (e.filename, e.strerror))


# Standard boilerplate to call the main() function to begin
# the program.
if __name__ == '__main__':
    main()