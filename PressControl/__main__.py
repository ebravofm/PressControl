from PressControl.press_control import cli
import argparse

def main():
    parser = argparse.ArgumentParser(description="Press Control")
    parser.add_argument("--work", help="Work", dest='work', action='store_true')
    
    args = parser.parse_args()
    print(args)
    print(args.work)
    
    cli(work=args.work)
    
if __name__ == "__main__":
    main()
