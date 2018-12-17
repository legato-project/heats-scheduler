from modules.envelope import main_loop_envelope
from modules.scheduler import main_loop_scheduler
from modules.context import Context
from multiprocessing import Process
from argparse import ArgumentParser
from logging import info
from sys import stderr


def main():

    parser = ArgumentParser(description='schedule pods')
    parser.add_argument('--rescheduling-time', nargs='?', const=60, metavar='rescheduling-nursery', type=int,
                        help='integer corresponding to the interval of rescheduling')
    parser.add_argument('--influx-user', nargs='?', const='root', metavar='influx-user', type=str,
                        help='string corresponding to the user name of the influx database')
    parser.add_argument('--influx-pass', nargs='?', const='root', metavar='influx-pass', type=str,
                        help='string corresponding to the password of the influx database')
    parser.add_argument('--influx-host', metavar='influx-host', type=str,
                        help='string corresponding to the host address of the influx database')
    parser.add_argument('--influx-port', metavar='influx-port', type=int,
                        help='int corresponding to the port of the influx database')
    parser.add_argument('--influx-database', nargs='?', const='k8s', metavar='influx-database', type=str,
                        help='string corresponding to the name of the influx database')
    parser.add_argument('-D', nargs='?', const='DEBUG', metavar='DEBUG', type=str,
                        help='Flag to decide if we want the debug mode or not')
    args = parser.parse_args()

    context = Context(args)

    process_scheduler = Process(target=main_loop_scheduler, args=(context,))
    process_envelope = Process(target=main_loop_envelope, args=(context,))

    process_scheduler.start()
    process_envelope.start()

    process_scheduler.join()
    process_envelope.join()


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        info("shutting down")
        pass
