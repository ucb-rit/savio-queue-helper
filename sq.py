import asyncio
import argparse
import datetime
import subprocess
import getpass
import pandas as pd
import shlex
import sys
from tabulate import tabulate
from termcolor import colored
from pathlib import Path
from os import path

def run_cmd(cmd):
    async def inner():
        proc = await asyncio.create_subprocess_shell(
            shlex.join(cmd), stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
        stdout, stderr = await proc.communicate()
        return stdout.decode('utf-8', 'backslashreplace')
    return asyncio.run(inner())

def freeze(dest_dir, name, data):
    with open(path.join(dest_dir, name), 'w') as f:
        f.write(data)

def read_slurm_as_df(stdout, sep = '|'):
    # The first line of the data is the column headers, the rest are job data entries
    # Rows are separated by newline, columns are separated by pipe |
    columns, *data = [ row.split(sep) for row in stdout.strip().split('\n') ]

    # Sometimes the fields can contain the pipe symbol |
    # In this case it is impossible to parse unambiguously so the row is thrown out
    # (potentially leading to incorrect results).
    # This could be improved by falling back to other commands (perhaps querying one field at a time)
    # if the result is not parsable, but this will increase load on Slurm.
    # 
    # In the wild, this is seen when the node status (from sinfo) is:
    #  NHC: check_fs_mount:  /tmp mount options incorrect (should match /(^|,)rw($|,)/)
    filtered_data = [ row for row in data if len(row) == len(columns) ]
    if len(data) != len(filtered_data):
      print(colored('Warning: Encountered unparsable data in Slurm command output. Some results shown here may be missing or incorrect. Use `squeue` for direct data from Slurm.', 'white', 'on_red'), file=sys.stderr)
      data = filtered_data

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

def load_from(load_file):
    def inner(f):
        def cmd(load_dir, *args):
            if load_dir:
                with open(path.join(load_dir, load_file), 'r') as file:
                    return file.read()
            return f(*args)
        return cmd
    return inner

@load_from('qos')
def qos_cmd():
    return run_cmd(['sacctmgr', 'show', 'QOS', '-P'])

def get_qos_df(stdout):
    return read_slurm_as_df(stdout)

@load_from('squeue')
def squeue_cmd():
    return run_cmd(['squeue', '--array-unique', '--format', '%all'])

def get_squeue_df(stdout):
    return read_slurm_as_df(stdout)

@load_from('sprio')
def sprio_cmd():
    return run_cmd(['sprio', '-o', '%A|%c|%F|%i|%J|%N|%Q|%r|%T|%u|%Y'])

def get_sprio_df(stdout):
    df = read_slurm_as_df(stdout)
    df['PRIORITY'] = df['PRIORITY'].astype(int)
    return df

@load_from('assoc')
def assoc_cmd():
    return run_cmd(['sacctmgr', '-p', 'show', 'associations'])

def get_assoc_df(stdout):
    return read_slurm_as_df(stdout)

@load_from('sinfo')
def sinfo_cmd():
    return run_cmd(['sinfo', '-N', '--format', '%N,%P'])
    ## return run_cmd(['sinfo', '--format', '%all'])

def get_sinfo_df(stdout):
    df = read_slurm_as_df(stdout, ',')
    df.columns = df.columns.str.strip()
    return df

@load_from('resv')
def resv_cmd():
    return run_cmd(['scontrol', 'show', 'reservation'])

def get_resv_df(stdout):
    if stdout.startswith('No reservations in the system'):
      return pd.DataFrame(columns=['Nodes','StartTime','EndTime'])
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

@load_from('recent_jobs')
def recent_jobs_cmd(username, start_time=None):
    last_week = (datetime.date.today() - datetime.timedelta(days=7)).isoformat()
    start_time = start_time or last_week
    return run_cmd(['sacct', '-u', username, '-P', '--format', 'ALL', '-S', start_time])

def get_recent_jobs(stdout, limit=-1):
    df = read_slurm_as_df(stdout) \
        .sort_values(by='End', ascending=False)
    df = df[~df['JobID'].str.endswith('.batch')]
    if limit >= 0:
        df = df.head(limit)
    return df

def cache_property(f):
    cached = None
    def inner(self, *args):
        nonlocal cached
        if cached is None:
            cached = f(self, *args)
        return cached
    return inner

class SlurmInfo:
    def __init__(self, load_dir):
        self.load_dir = load_dir

    @cache_property
    def squeue_df(self):
        return get_squeue_df(squeue_cmd(self.load_dir))

    @cache_property
    def qos_df(self):
        return get_qos_df(qos_cmd(self.load_dir))

    @cache_property
    def sprio_df(self):
        return get_sprio_df(sprio_cmd(self.load_dir))

    @cache_property
    def resv_df(self):
        return get_resv_df(resv_cmd(self.load_dir))

    @cache_property
    def sinfo_df(self):
        return get_sinfo_df(sinfo_cmd(self.load_dir))
    
    @cache_property
    def assoc_df(self):
        return get_assoc_df(assoc_cmd(self.load_dir))

    @cache_property
    def get_recent_jobs(self, username, start_time=None, limit=-1):
        return get_recent_jobs(recent_jobs_cmd(self.load_dir, username, start_time), limit)

    def has_current_jobs(self, username):
        return self.squeue_df()[self.squeue_df()['USER'] == username].shape[0] > 0
    def available_qos(self, username, account, partition):
        return self.assoc_df()[
            (self.assoc_df()['User'] == username) &
            (self.assoc_df()['Account'] == account) &
            (self.assoc_df()['Partition'] == partition)
        ]['QOS'].str.split(',').values[0]
    def freeze(self, dest_dir, username, start_time):
        Path(dest_dir).mkdir(parents=True, exist_ok=True)
        freeze(dest_dir, 'squeue', squeue_cmd(self.load_dir))
        freeze(dest_dir, 'qos', qos_cmd(self.load_dir))
        freeze(dest_dir, 'sprio', sprio_cmd(self.load_dir))
        freeze(dest_dir, 'resv', resv_cmd(self.load_dir))
        freeze(dest_dir, 'sinfo', sinfo_cmd(self.load_dir))
        freeze(dest_dir, 'assoc', assoc_cmd(self.load_dir))
        freeze(dest_dir, 'recent_jobs', recent_jobs_cmd(self.load_dir, username, start_time))

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
    df['Name'] = df['JobName']
    df['Job ID'] = df.apply(display_job_id, axis=1)
    df['Nodes'] = df['NNodes'] + 'x ' + df['Partition']
    df['State'] = df['State'].apply(color_state)
    df['End'] = df['End'].str.replace('T', ' ')
    df = df[['Job ID', 'Name', 'Account', 'Nodes', 'Elapsed', 'End', 'State']].sort_values(by='End', ascending=False)
    if df.shape[0] == 0:
        print('No recent jobs (within the last week).')
    else:
        print('Recent jobs (most recent job first):')
        print(get_table(df))
        # print('For more information about a previous job, run:', 'sacct -j $JOB_ID')

    df = sacct.copy()
    df['JOBID'] = df['JobID']
    display_problems(df, quiet)


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
        if ':' in kv:
            tmp = kv.split(':')
            # Modified to account for "gres:V100:2"
            # key, value = kv.split(':')
            key = tmp[0]
            tres[key] = int(tmp[-1])  # int(value)
    return tres

def parse_tres_nodes(nodes, tres_per_node, other):
    tres = parse_tres(tres_per_node)
    tres = { 'gres/' + k: v * int(nodes) for k, v in tres.items() }
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
    return ', '.join(sorted(tres_strs, key=lambda x: x.split(' ')[1]))

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
    if len(non_resv_nodes) < remaining_num_nodes:
        interfering_resvs += general_interfering_resvs

    return interfering_resvs

def suggest_other_qos(slurm_info, pending_job):
    available_qos = slurm_info.available_qos(pending_job['USER'], pending_job['ACCOUNT'], pending_job['PARTITION'])
    available_qos.remove(pending_job['QOS'])
    return display_qos_suggestion(available_qos)

def display_qos_suggestion(available_qos):
    return f"You may consider submitting with these other QOS: {', '.join(available_qos)}" if available_qos else ''

def identify_problems(slurm_info):
    def inner(pending_job):
        severity = SEVERITY['LOW']
        problems = []
        if pending_job['REASON'] in ['QOSMaxWallDurationPerJobLimit']:
            qos = slurm_info.qos_df()[slurm_info.qos_df()['Name'] == pending_job['QOS']].iloc[0]
            job_wall_time = pending_job['TIME_LIMIT']
            severity = max(severity, SEVERITY['HIGH'])
            problems.append([
                f"This job will not run because its time limit exceeds the maximum time limit for jobs under the QOS {pending_job['QOS']}.",
                f"QOS maximum time limit: {qos['MaxWall']}",
                f"This job time limit: {job_wall_time}",
                "You should cancel this job and resubmit with a lower wall clock time limit or use a different QOS with a higher allowed time limit.",
                # TODO: suggest QOS with higher time if applicable
            ])
        if pending_job['REASON'] in ['QOSMaxCpuPerJobLimit', 'QOSMaxNodePerJobLimit']:
            qos = slurm_info.qos_df()[slurm_info.qos_df()['Name'] == pending_job['QOS']].iloc[0]
            qos_resource_limit = parse_tres(qos['MaxTRES'])
            qos_resource_limit_str = display_grp_tres(qos_resource_limit)
            job_resources_requested = display_grp_tres(filter_keys(qos_resource_limit, parse_tres_queue_job(pending_job)))

            severity = max(severity, SEVERITY['HIGH'])
            problems.append([
                f"This job will not run because it is requesting more resources than is allowed per job under the QOS {pending_job['QOS']}.",
                f"QOS per job resource limit: {qos_resource_limit_str}",
                f"This job is requesting: {job_resources_requested}",
                "You should cancel this job and resubmit with lower resource requirements or use a different QOS.",
                suggest_other_qos(slurm_info, pending_job)
            ])
        if pending_job['REASON'] in ['QOSGrpNodeLimit', 'QOSGrpCpuLimit', 'QOSGrpGRES']:
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
            problems.append([
                f"This job is waiting until the QOS {pending_job['QOS']} has available resources.",
                f"QOS resource limit: {qos_resource_limit_str}",
                f"QOS currently using: {qos_resources_used}: {qos_running_jobs_str}",
                f"This job is requesting: {job_resources_requested}",
                suggest_other_qos(slurm_info, pending_job)
            ])
        if pending_job['REASON'] in ['Priority', 'Resources'] or re.match("^ReqNodeNotAvail", pending_job['REASON']):
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
    print(f"Currently {num_running_jobs} running {'job' if num_running_jobs == 1 else 'jobs'} and {num_pending_jobs} pending {'job' if num_pending_jobs == 1 else 'jobs'} (most recent job first):")

    display_df = pd.DataFrame()
    display_df['Job ID'] = df.apply(display_job_id, axis=1)
    display_df['Name'] = df['NAME']
    display_df['Account'] = df['ACCOUNT']
    display_df['Nodes'] = df['NODES'] + 'x ' + df['PARTITION']
    display_df['QOS'] = df['QOS']
    display_df['Time'] = df['TIME']
    display_df['State'] = df['STATE'].apply(color_state)
    display_df['Reason'] = df['REASON'].apply(color_reason)

    print(get_table(display_df))
    display_problems(df, quiet)

def display_problems(df, quiet):
    if not quiet:
        problem_jobs = df[df['PROBLEMS'].apply(lambda p: len(p[1]) > 0)]
        for _, job in problem_jobs.iterrows():
            print('\n' + display_job_id(job) + ':')
            severity, problems = job['PROBLEMS']
            for problem in problems:
                print(' - ' + '\n   '.join([ p for p in problem if p ]) if type(problem) == list else problem)

def identify_problems_completed(slurm_info):
    def inner(completed_job):
        severity = SEVERITY['LOW']
        problems = []
        elapsed_time = parse_timelimit(completed_job['Elapsed'])
        if (completed_job['State'] == 'COMPLETED') and elapsed_time < datetime.timedelta(minutes=5):
            severity = max(severity, SEVERITY['MEDIUM'])
            problems.append([
                f"This job ran for a very short amount of time ({elapsed_time}). You may want to check that the output was correct or if it exited because of a problem."
            ])
        return severity, problems
    return inner

parser = argparse.ArgumentParser(description='Display pending job/queue info in a helpful way.')
parser.add_argument('-u', '--user', help='Set username to check (default is current user)', default=getpass.getuser())
parser.add_argument('-a', '--all-jobs', dest='all_jobs', default=False, action='store_true', help='Show current and past jobs for selected user')
parser.add_argument('-q', '--quiet', dest='quiet', default=False, action='store_true', help='Suppress job issue messages')
parser.add_argument('-S', '--start-time', dest='start_time', help='Filter for jobs created after specified start time', type=str, default=None)
parser.add_argument('-n', '--limit', dest='limit', type=int, default=8, help='Limit number of jobs in job history table (unlimited = -1, default = 8)')
parser.add_argument('--freeze', type=str, help='(debug) Freeze Slurm command outputs to a directory')
parser.add_argument('--load', type=str, help='(debug) Load results from a frozen Slurm output directory')
args = parser.parse_args()

slurm_info = SlurmInfo(args.load)

username = args.user

print('Showing results for user', username)

if args.freeze:
    slurm_info.freeze(args.freeze, username, args.start_time)

if slurm_info.has_current_jobs(username) and (not args.all_jobs):
    display_queued_jobs(username, slurm_info, args.quiet)
    print()
elif (not args.all_jobs):
    print('No running or queued jobs.')

try:
    recent_jobs = slurm_info.get_recent_jobs(username, args.start_time, args.limit)
    if recent_jobs.shape[0]:
        recent_jobs['PROBLEMS'] = recent_jobs.apply(identify_problems_completed(slurm_info), axis=1)

        recent_completed = recent_jobs[recent_jobs['State'] == 'COMPLETED'].sort_values(by='End', ascending=False).head(1)
        has_problems = recent_completed[recent_completed['PROBLEMS'].apply(lambda problems: len(problems[1]) > 0)]

        if args.all_jobs or (not slurm_info.has_current_jobs(username)):
            display_recent_jobs(recent_jobs, args.quiet)
        elif has_problems.shape[0]:
            # just show the most recent one with the problem
            display_recent_jobs(recent_completed, args.quiet)
except:
    pass
