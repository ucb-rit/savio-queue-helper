import argparse
import datetime
import subprocess
import getpass
import pandas as pd
from tabulate import tabulate
from termcolor import colored

def read_slurm_as_df(slurm_command):
    stdout = subprocess.check_output(slurm_command).decode('utf-8')

    # The first line of the data is the column headers, the rest are job data entries
    # Rows are separated by newline, columns are separated by pipe |
    columns, *data = [ row.split('|') for row in stdout.strip().split('\n') ]

    df = pd.DataFrame(data=data, columns=columns)
    columns = []
    column_counts = {}
    for col in df.columns:
        if col in column_counts:
            column_counts[col] += 1
            col = col + '_' + str(column_counts[col])
        if col not in column_counts:
            column_counts[col] = 1
        columns.append(col)
    df.columns = columns
    return df

def get_qos_df():
    return read_slurm_as_df(['sacctmgr', 'show', 'QOS', '-P'])

def get_squeue_df():
    return read_slurm_as_df(['squeue', '--array-unique', '--format', '%all'])

def get_sprio_df():
    return read_slurm_as_df(['sprio', '-o', '%A|%c|%F|%i|%J|%N|%Q|%r|%T|%u|%Y'])

def get_assoc_df():
    return read_slurm_as_df(['sacctmgr', '-p', 'show', 'associations'])

def get_sinfo_df():
    df = read_slurm_as_df(['sinfo', '--format', '%all'])
    df.columns = df.columns.str.strip()
    return df

def get_resv_df():
    stdout = subprocess.check_output(['scontrol', 'show', 'reservation']).decode('utf-8')
    resvs = []
    for line in stdout.split('\n'):
        if line and not line.startswith(' '):
            resvs.append({})
        for word in line.strip().split(' '):
            key, value = word[:word.find('=')], word[word.find('=')+1:]
            if key and value:
                resvs[-1][key] =value
    df = pd.DataFrame(resvs)
    df['Nodes'] = df['Nodes'].str.split(',')
    df['StartTime'] = df['StartTime'].apply(datetime.datetime.fromisoformat)
    df['EndTime'] = df['EndTime'].apply(datetime.datetime.fromisoformat)
    return df

def get_recent_jobs(username, start_time=None, limit=-1):
    last_week = (datetime.date.today() - datetime.timedelta(days=7)).isoformat()
    start_time = start_time or last_week
    df = read_slurm_as_df(['sacct', '-u', username, '-P', '--format', 'ALL', '-S', start_time])\
        .sort_values(by='End', ascending=False)
    df = df[~df['JobID'].str.endswith('.batch')]
    if limit >= 0:
        df = df.head(limit)
    return df

def cache_property(f):
    cached = None
    def inner(self):
        nonlocal cached
        if cached is None:
            cached = f()
        return cached
    return inner

class SlurmInfo:
    @cache_property
    def squeue_df():
        return get_squeue_df()

    @cache_property
    def qos_df():
        return get_qos_df()

    @cache_property
    def sprio_df():
        return get_sprio_df()

    @cache_property
    def resv_df():
        return get_resv_df()

    @cache_property
    def sinfo_df():
        return get_sinfo_df()
    
    @cache_property
    def assoc_df():
        return get_assoc_df()

    def has_current_jobs(self, username):
        return self.squeue_df()[self.squeue_df()['USER'] == username].shape[0] > 0
    def available_qos(self, username, account, partition):
        return self.assoc_df()[
            (self.assoc_df()['User'] == username) &
            (self.assoc_df()['Account'] == account) &
            (self.assoc_df()['Partition'] == partition)
        ]['QOS'].str.split(',').values[0]

def get_table(df):
    return tabulate(df, df.columns, tablefmt='pretty', showindex='never')

def color_state(state):
    colors = {
        'PENDING':'yellow',
        'RUNNING': 'green',
        'COMPLETED': 'cyan',
        'COMPLETING': 'cyan',
        'CANCELLED': 'magenta',
        'TIMEOUT': 'red',
        'RESV_DEL_HOLD': 'red',
        'OUT_OF_MEMORY': 'red',
        'NODE_FAIL': 'red',
        'FAILED': 'red',
    }
    state = state.split(' ')[0].strip()
    return colored(state, colors[state]) if state in colors else state

def color_reason(reason):
    colors = {
    }
    if reason == 'None':
        return ''
    return colored(reason, colors[reason]) if reason in colors else reason

def display_time(time):
    return '' if time == '0:00' else time

def display_recent_jobs(sacct, quiet):
    df = sacct.copy()
    df['JOBID'] = df['JobID']
    df['Job ID'] = df.apply(display_job_id, axis=1)
    df['Partition'] = df['NNodes'] + 'x ' + df['Partition']
    df['State'] = df['State'].apply(color_state)
    df['End'] = df['End'].str.replace('T', ' ')
    df = df[['JobID', 'JobName', 'Account', 'Partition', 'Elapsed', 'End', 'State']].sort_values(by='End', ascending=False)
    if df.shape[0] == 0:
        print('You have no recent jobs (within the last week).')
    else:
        print('Your recent jobs (most recent job first):')
        print(get_table(df))
        # print('For more information about a previous job, run:', 'sacct -j $JOB_ID')

    df = sacct.copy()
    if not quiet:
        problem_jobs = df[df['PROBLEMS'].apply(lambda p: len(p[1]) > 0)]
        for _, job in problem_jobs.iterrows():
            print('\n' + job['JobID'] + ':')
            severity, problems = job['PROBLEMS']
            for problem in problems:
                print(' -', problem)


def display_job_id(pending_job):
    SEVERITY_COLORS = {
        SEVERITY['LOW']: [],
        SEVERITY['MEDIUM']: ['grey', 'on_yellow'],
        SEVERITY['HIGH']: ['white', 'on_red']
    }
    color = SEVERITY_COLORS[pending_job['PROBLEMS'][0]]
    return colored(pending_job['JOBID'], *color) if color else pending_job['JOBID']

def qos_node_limit(qos_df):
    return df['GrpTRES'].apply(lambda n: n.split('=')[1])

def parse_tres(tres_str):
    tres = {}
    for kv in tres_str.split(','):
        if '=' in kv:
            key, value = kv.split('=')
            tres[key] = int(value)
    return tres

def parse_tres_nodes(nodes, tres_per_node, other):
    tres = parse_tres(tres_per_node)
    tres = { k: v * int(nodes) for k, v in tres.items() }
    tres['node'] = int(nodes)
    for key, value in other.items():
        tres[key] = int(value)
    return tres

def parse_tres_queue_job(queue_job):
    return parse_tres_nodes(queue_job['NODES'], queue_job['TRES_PER_NODE'], {
        'cpu': queue_job['CPUS']
    })

def display_grp_tres(tres):
    tres_strs = []
    for key, value in tres.items():
        if value == '1' and key.endswith('s'):
            key = key[:-1]
        if not key.endswith('s') and value != '1':
            key = key + 's'
        tres_strs.append(f"{value} {key}")
    return ', '.join(tres_strs)

def sum_dicts(dicts):
    total = {}
    for d in dicts:
        for k, v in d.items():
            if k not in total:
                total[k] = 0
            total[k] += v
    return total

def filter_keys(keys, obj):
    return { k: v for k, v in obj.items() if k in keys }

SEVERITY = {
    'LOW': 0,
    'MEDIUM': 1,
    'HIGH': 2
}

def parse_timelimit(timelimit_str):
    def safe_get(arr, i, default=0):
        if len(arr) > i:
            return int(arr[i])
        return default

    days, hours, minutes, seconds = 0, 0, 0, 0
    days = timelimit_str.split('-')[0] if '-' in timelimit_str else 0
    if '-' in timelimit_str:
        days = int(timelimit_str.split('-')[0])
        timelimit_str = timelimit_str.split('-')[1]
    else:
        days = 0
    parts = timelimit_str.split(':')
    hours, minutes, seconds = safe_get(parts, 0), safe_get(parts, 1), safe_get(parts, 2)
    return datetime.timedelta(days=days, hours=hours, minutes=minutes, seconds=seconds)

def check_resv_conflicts(pending_job, resv_df, sinfo_df):
    req_nodes = pending_job['REQ_NODES'].split(',') if pending_job['REQ_NODES'] else []
    num_nodes = int(pending_job['NODES'])
    time_limit = parse_timelimit(pending_job['TIME_LIMIT'])
    earliest_start = datetime.datetime.now()
    earliest_end = datetime.datetime.now() + time_limit

    # For each node slot, is there a node available to fill it that is not in a reservation?
    # Check if each of the requested nodes does not intersect with a partition
    #
    # For the remaining slots which are not requested nodes,
    # check if there are at least that many that are not overlapping with
    # any of the interfering reservations

    resvs = resv_df[
        ((resv_df['StartTime'] <= earliest_start) & (resv_df['EndTime'] >= earliest_start))
        | ((resv_df['StartTime'] <= earliest_end) & (resv_df['EndTime'] >= earliest_end))]

    interfering_resvs = []
    for node in req_nodes:
        for _, resv in resvs.iterrows():
            if node in resv['Nodes']:
                interfering_resvs.append(resv)


    possible_nodes = sinfo_df[sinfo_df['PARTITION'] == pending_job['PARTITION']]['NODELIST']
    remaining_num_nodes = num_nodes - len(req_nodes)
    non_resv_nodes = set(possible_nodes)
    general_interfering_resvs = []
    for _, resv in resvs.iterrows():
        delta = set(resv['Nodes']).intersection(set(possible_nodes))
        non_resv_nodes = non_resv_nodes - set(resv['Nodes'])
        if delta:
            general_interfering_resvs.append(resv)
    if len(non_resv_nodes) >= remaining_num_nodes:
        interfering_resvs += general_interfering_resvs

    return interfering_resvs

def identify_problems(slurm_info):
    def inner(pending_job):
        severity = SEVERITY['LOW']
        problems = []
        if pending_job['REASON'] in ['QOSGrpNodeLimit', 'QOSGrpCpuLimit']:
            qos_df = slurm_info.qos_df()
            queue = slurm_info.squeue_df()
            qos_df = qos_df[qos_df['Name'] == pending_job['QOS']]

            qos_resource_limit = parse_tres(qos_df['GrpTRES'].values[0])
            qos_resource_limit_str = display_grp_tres(parse_tres(qos_df['GrpTRES'].values[0]))
            qos_running_jobs = queue[(queue['QOS'] == pending_job['QOS']) & (queue['STATE'] == 'RUNNING')]
            qos_running_jobs_str = ', '.join(qos_running_jobs['JOBID'] + ' (' + qos_running_jobs.apply(lambda x: filter_keys(qos_resource_limit, parse_tres_queue_job(x)), axis=1).apply(display_grp_tres) + ')')

            qos_resources_used = display_grp_tres(filter_keys(qos_resource_limit, sum_dicts(qos_running_jobs.apply(parse_tres_queue_job, axis=1))))
            job_resources_requested = display_grp_tres(filter_keys(qos_resource_limit, parse_tres_queue_job(pending_job)))

            available_qos = slurm_info.available_qos(pending_job['USER'], pending_job['ACCOUNT'], pending_job['PARTITION'])
            available_qos.remove(pending_job['QOS'])
            other_qos = f"\n    You may consider submitting with these other QOS: {', '.join(available_qos)}" if available_qos else ''

            severity = max(severity, SEVERITY['MEDIUM'])
            problems.append(f"""This job is waiting until the QOS {pending_job['QOS']} has available resources.
    QOS limit: {qos_resource_limit_str}
    QOS currently using: {qos_resources_used}: {qos_running_jobs_str}
    This job is requesting: {job_resources_requested} {other_qos}""")
        if pending_job['REASON'] in ['Priority', 'Resources']:
            sprio_df = slurm_info.sprio_df()
            resv_conflicts = check_resv_conflicts(pending_job, slurm_info.resv_df(), slurm_info.sinfo_df())
            if resv_conflicts:
                severity = max(severity, SEVERITY['HIGH'])
                resvs = set([ (resv['ReservationName'], resv['StartTime'], resv['EndTime']) for resv in resv_conflicts ])
                problems.append(f"""This job has a maximum time limit of {pending_job['TIME_LIMIT']}.
    This job conflicts with the following reservations:\n""" +
                                '\n'.join([ f'   - {resv[0]}: {resv[1]} to {resv[2]}' for resv in resvs ]) +
                                """
    This job will run AFTER the reservation.
    Reservations are used for cluster maintenance.
    If the reservation has not yet started, then you could decrease the wall-clock time for the job so it terminates before the reservation starts.""")
            else:
                severity = max(severity, SEVERITY['LOW'])
                # pending_partition = queue[(queue['PARTITION'] == pending_job['PARTITION']) & (queue['STATE'] == 'PENDING')]
                # pending_demand = display_grp_tres(sum_dicts(pending_partition.apply(parse_tres_queue_job, axis=1).values))
                this_priority = sprio_df[sprio_df['JOBID'] == pending_job['JOBID_2']]['PRIORITY'].values[0]
                higher_prio_jobs = sprio_df[
                    (sprio_df['PARTITION'] == pending_job['PARTITION']) & \
                    ((sprio_df['PRIORITY'] > this_priority) | \
                    ((sprio_df['PRIORITY'] == this_priority) & (sprio_df['JOBID'] < pending_job['JOBID'])))]
                problems.append(f"""This job is scheduled to run after {higher_prio_jobs.shape[0]} higher priority jobs.
    Estimated start time: {pending_job['START_TIME']}
    To get scheduled sooner, you can try reducing wall clock time as appropriate.""")
        return severity, problems
    return inner

def display_queued_jobs(username, slurm_info, quiet):
    df = slurm_info.squeue_df().copy()
    df = df[df['USER'] == username]
    df['PROBLEMS'] = df.apply(identify_problems(slurm_info), axis=1)

    num_running_jobs = df[df['STATE'] == 'RUNNING'].shape[0]
    num_pending_jobs = df[df['STATE'] == 'PENDING'].shape[0]
    print(f"You have {num_running_jobs} running {'job' if num_running_jobs == 1 else 'jobs'} and {num_pending_jobs} pending {'job' if num_pending_jobs == 1 else 'jobs'} (most recent job first):")

    display_df = pd.DataFrame()
    display_df['Job ID'] = df.apply(display_job_id, axis=1)
    display_df['Name'] = df['NAME']
    display_df['Account'] = df['ACCOUNT']
    display_df['Partition'] = df['NODES'] + 'x ' + df['PARTITION']
    display_df['QOS'] = df['QOS']
    display_df['Time'] = df['TIME']
    display_df['State'] = df['STATE'].apply(color_state)
    display_df['Reason'] = df['REASON'].apply(color_reason)

    print(get_table(display_df))

    if not quiet:
        problem_jobs = df[df['PROBLEMS'].apply(lambda p: len(p[1]) > 0)]
        for _, job in problem_jobs.iterrows():
            print('\n' + job['JOBID'] + ':')
            severity, problems = job['PROBLEMS']
            for problem in problems:
                print(' -', problem)

def identify_problems_completed(slurm_info):
    def inner(completed_job):
        severity = SEVERITY['LOW']
        problems = []
        elapsed_time = parse_timelimit(completed_job['Elapsed'])
        if elapsed_time < datetime.timedelta(minutes=5):
            severity = max(severity, SEVERITY['MEDIUM'])
            problems.append(f"This job ran for only {elapsed_time}")
        return severity, problems
    return inner

parser = argparse.ArgumentParser(description='Display pending job/queue info in a helpful way.')
parser.add_argument('-u', '--user', help='Set username to check (default is current user)', default=getpass.getuser())
parser.add_argument('-a', '--all-jobs', dest='all_jobs', default=False, action='store_true', help='Show current and past jobs for selected user')
parser.add_argument('-q', '--quiet', dest='quiet', default=False, action='store_true', help='Suppress job issue messages')
parser.add_argument('-S', '--start-time', dest='start_time', help='Filter for jobs created after specified start time', type=str, default=None)
parser.add_argument('-n', '--limit', dest='limit', type=int, default=8, help='Limit number of jobs in job history table (unlimited = -1, default = 8)')
args = parser.parse_args()

slurm_info = SlurmInfo()

username = args.user

print('Showing results for', username)

if slurm_info.has_current_jobs(username) and (not args.all_jobs):
    display_queued_jobs(username, slurm_info, args.quiet)
    print()
elif (not args.all_jobs):
    print('You have no running or queued jobs.')

recent_jobs = get_recent_jobs(username, args.start_time, args.limit)
recent_jobs['PROBLEMS'] = recent_jobs.apply(identify_problems_completed(slurm_info), axis=1)

recent_completed = recent_jobs[recent_jobs['State'] == 'COMPLETED'].sort_values(by='End', ascending=False).head(1)
has_problems = recent_completed[recent_completed['PROBLEMS'].apply(lambda problems: len(problems[1]) > 0)]

if args.all_jobs or (not slurm_info.has_current_jobs(username)):
    display_recent_jobs(recent_jobs, args.quiet)
elif has_problems.shape[0]:
    # just show the most recent one with the problem
    display_recent_jobs(recent_completed, args.quiet)