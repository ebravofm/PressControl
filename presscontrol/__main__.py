from presscontrol.press_control import cli
import argparse

def main():
    parser = argparse.ArgumentParser(description="Press Control")
    parser.add_argument("--work", 
                        help="Work", 
                        dest='work', 
                        action='store_true')
    parser.add_argument("--scrape", 
                        help="Scrape twitter page", 
                        dest='scrape', 
                        action='store_true')    
    parser.add_argument("--user-scrape",
                        help="Scrape using user scraping method, specify name.",
                        dest="user_scrape",
                        default=False,
                        type=str)

    # Arguments for twitter scraping
    parser.add_argument("--display", 
                        help="Display scraping result", 
                        dest='display', action='store_true')
    
    parser.add_argument("--save", 
                        help="Save scraping result", 
                        dest='save',
                        action='store_true')

    parser.add_argument("--update",
                        help="Update scraping result to DB",
                        dest='update',
                        action='store_true')

    parser.add_argument("--print-sum",
                        help="Print Scraping Summary",
                        dest='print_sum',
                        action='store_true')

    parser.add_argument("--dump-sum",
                        help="Dump Scraping Summary",
                        dest='dump_sum',
                        action='store_true')

    parser.add_argument("-filename",
                        help="Filename for scraping result .csv",
                        dest="file_name",
                        default=None,
                        type=str)

    parser.add_argument("-username",help="Twitter Username",
                        dest="username",
                        default='',
                        type=str)

    parser.add_argument("-since",help="Since for twitter scraping",
                        dest="since",
                        default='',
                        type=str)

    parser.add_argument("-until",help="Until for twitter scraping",
                        dest="until",
                        default='',
                        type=str)

    parser.add_argument("-years",
                        help="Years ago for twitter scraping",
                        dest="years",
                        default=0,
                        type=int)

    parser.add_argument("-months",
                        help="Months ago for twitter scraping",
                        dest="months",
                        default=0,
                        type=int)

    parser.add_argument("-days",
                        help="Days ago for twitter scraping",
                        dest="days",
                        default=0,
                        type=int)
    
    args = parser.parse_args()

    if args.scrape is True:
        if args.username == '': parser.error('Username for scraping cannot be empty.')
        if args.since == '' and args.until == '' and args.years == 0 and args.months == 0 and args.days == 0: parser.error('At least one of these (-since -until -years -months -days) must be not null.')
    
    cli(work=args.work, 
        scrape=args.scrape,
        user_scrape=args.user_scrape,
        display=args.display,
        save=args.save,
        print_sum=args.print_sum,
        dump_sum=args.dump_sum,
        update=args.update,
        file_name=args.file_name,
        username=args.username,
        since=args.since,
        until=args.until,
        years=args.years,
        months=args.months,
        days=args.days)
    
if __name__ == "__main__":
    main()