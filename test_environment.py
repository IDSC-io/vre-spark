import sys

REQUIRED_PYTHON = "python"
REQUIRED_MAJOR = 3


def main():
    system_major = sys.version_info.major

    if system_major != REQUIRED_MAJOR:
        raise TypeError(
            "This project requires Python {}. Found: Python {}".format(
                REQUIRED_MAJOR, sys.version))
    else:
        print(">>> Development environment passes all tests!")


if __name__ == '__main__':
    main()
