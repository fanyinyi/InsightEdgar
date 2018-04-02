'''
This is the code challenge for the InsightDataScience, the script includes two class and two subfunctions and
one main function. Classes include session and request as the website indicates us to do so. "kick_inactive_session"
function is the core operation of the whole project and "process_stream" function is the main logical of our project we
handle the data line by line just as real time stream data.
'''

import datetime

class Session:
    '''
    session includes ip, start time, end time and numbers of docs. If a new request comes up, it may need update its
    end time and number of docs should plus one since it made a request on a new web page.
    '''
    def __init__(self, request):
        self.ip = request.ip
        self.start_time = request.time
        self.end_time = request.time
        self.num_docs = 1
        self.counter = 0

    def update(self, request):
        self.end_time = request.time
        self.num_docs += 1


class Request:
    '''
    request includes the highlighed areas just as mention in the github code challenge documents page: it has ip, time of
    request, cik, accession, extension that all indicates the specific web pages that requested from the users. Although
    not all the areas are used, I made a complete class for the all areas that highlighted on the code challenge
    documents page.
    '''
    def __init__(self, line):
        log_entry = line.split(',')

        self.ip = log_entry[0].replace('"', '')
        self.time = datetime.datetime.strptime(log_entry[1].replace('"', '') + ' ' + log_entry[2].replace('"', ''),
                                               '%Y-%m-%d %H:%M:%S')
        self.cik = log_entry[4].replace('"', '')
        self.accession = log_entry[5].replace('"', '')
        self.extension = log_entry[6].replace('"', '')


def kick_inactive_session(kick_ip, active_session, out_file):
    '''
    kick the inactive session given ip and write out to files
    :param kick_ip: list of ips that should be output
    :param active_session: dict of keys - request,ip and value - sessions
    :param out_file: the text we need to output our results
    :return: the file that holds our answers
    '''
    if kick_ip is None:
        return

    kick_ip = list(kick_ip)

    kick_sessions = []
    for ip in kick_ip:
        kick_sessions.append(active_session.pop(ip))

    kick_sessions.sort(key=lambda x: (x.start_time, x.end_time, x.counter))

    # write sessions to file
    for session in kick_sessions:
        duration = str(int((session.end_time - session.start_time).total_seconds()) + 1)
        start_time = session.start_time.strftime('%Y-%m-%d %H:%M:%S')
        end_time = session.end_time.strftime('%Y-%m-%d %H:%M:%S')
        num_docs = str(session.num_docs)
        line = [session.ip, start_time, end_time, duration, num_docs]

        out_file.write(",".join(line))
        out_file.write("\n")


def process_stream(log_file, inactivity_period, out_file):
    '''
    process the data line by line: a new request will determine whether the previous data will be output or not
    :param log_file: the input file we need to process line by line
    :param inactivity_period: a single value denoting the period of inactivity that should be used to identify
    when a user session is over
    :param out_file:  the text we need to output our results
    :return: this is our main logical function, this will help us do the all the process of stream data and then output
    '''
    active_session = {}

    next(log_file)
    current_time = datetime.datetime.min
    kick_schedule = {}
    counter = 0
    for line in log_file:
        request = Request(line)
        counter += 1
        # update current time
        if current_time != request.time:
            if current_time in kick_schedule:
                kick_ip = kick_schedule.pop(current_time, None)
                kick_inactive_session(kick_ip, active_session, out_file)
            current_time = request.time

        # process session from request
        if request.ip not in active_session:
            session = Session(request)
            session.counter = counter
            active_session[request.ip] = session
        else:
            # get correct kick time
            prev_kick_time = active_session[request.ip].end_time + datetime.timedelta(seconds=inactivity_period)
            kick_schedule[prev_kick_time].remove(request.ip)
            active_session[request.ip].update(request)

        # schedule the time that ip will be kicked
        kick_time = current_time + datetime.timedelta(seconds=inactivity_period)
        if kick_time not in kick_schedule:
            kick_schedule[kick_time] = set()
        kick_schedule[kick_time].add(request.ip)

    # process remain active_session
    kick_ip = active_session.keys()
    kick_inactive_session(kick_ip, active_session, out_file)


def main():
    log_file = open('input/log.csv', 'r')
    inactivity_period = open('input/inactivity_period.txt', 'r')
    inactivity_period_time = inactivity_period.read()
    out_file = open("output/sessionization.txt", "w")
    process_stream(log_file, int(inactivity_period_time), out_file)
    log_file.close()
    out_file.close()


if __name__ == "__main__":
    main()
