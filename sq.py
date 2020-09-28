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

def get_resv_df():
    stdout = subprocess.check_output(['scontrol', 'show', 'reservation']).decode('utf-8')
    resvs = []
    for line in stdout.split('\n'):
        if line and not line.startswith(' '):
            resvs.append({})
        for word in line.strip().split(' '):
            print(word)
            key, value = word[:word.find('=')], word[word.find('=')+1:]
            if key and value:
                resvs[-1][key] =value
    print(pd.DataFrame(resvs))

# get_resv_df()

def get_recent_jobs(username):
    last_week = datetime.date.today() - datetime.timedelta(days=7)
    df = read_slurm_as_df(['sacct', '-u', username, '-P', '--format', 'ALL', '-S', last_week.isoformat()])
    return df[~df['JobID'].str.endswith('.batch')]

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

def display_recent_jobs(sacct):
    df = sacct.copy()
    df['Partition'] = df['Partition'] + ' (' + df['NNodes'] + ')'
    df['State'] = df['State'].apply(color_state)
    df['End'] = df['End'].str.replace('T', ' ')
    df = df[['JobID', 'JobName', 'Account', 'Partition', 'Elapsed', 'End', 'State']].sort_values(by='End', ascending=False)
    if df.shape[0] == 0:
        print('You have no recent jobs (within the last week).')
    else:
        print('Your recent jobs (most recent job first):')
        print(get_table(df))
        # print('For more information about a previous job, run:', 'sacct -j $JOB_ID')

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

def identify_problems(pending_job, queue, qos_df, sprio_df):
    severity = SEVERITY['LOW']
    problems = []
    if pending_job['REASON'] in ['QOSGrpNodeLimit', 'QOSGrpCpuLimit']:

        qos_df = qos_df[qos_df['Name'] == pending_job['QOS']]

        qos_resource_limit = parse_tres(qos_df['GrpTRES'].values[0])
        qos_resource_limit_str = display_grp_tres(parse_tres(qos_df['GrpTRES'].values[0]))
        qos_running_jobs = queue[(queue['QOS'] == pending_job['QOS']) & (queue['STATE'] == 'RUNNING')]
        qos_running_jobs_str = ', '.join(qos_running_jobs['JOBID'] + ' (' + qos_running_jobs.apply(lambda x: filter_keys(qos_resource_limit, parse_tres_queue_job(x)), axis=1).apply(display_grp_tres) + ')')

        qos_resources_used = display_grp_tres(filter_keys(qos_resource_limit, sum_dicts(qos_running_jobs.apply(parse_tres_queue_job, axis=1))))
        job_resources_requested = display_grp_tres(filter_keys(qos_resource_limit, parse_tres_queue_job(pending_job)))

        severity = max(severity, SEVERITY['MEDIUM'])
        problems.append(f"""This job is waiting until the QOS {pending_job['QOS']} has available resources.
   QOS limit: {qos_resource_limit_str}
   QOS currently using: {qos_resources_used}: {qos_running_jobs_str}
   This job is requesting: {job_resources_requested}.
   You may consider submitting with lowprio QOS, but then your job would be subject to preemption.""")
    if pending_job['REASON'] == 'Priority':
        severity = max(severity, SEVERITY['LOW'])
        # pending_partition = queue[(queue['PARTITION'] == pending_job['PARTITION']) & (queue['STATE'] == 'PENDING')]
        # pending_demand = display_grp_tres(sum_dicts(pending_partition.apply(parse_tres_queue_job, axis=1).values))
        this_priority = sprio_df[sprio_df['JOBID'] == pending_job['JOBID_2']]['PRIORITY'].values[0]
        higher_prio_jobs = sprio_df[
            (sprio_df['PARTITION'] == pending_job['PARTITION']) & \
            ((sprio_df['PRIORITY'] > this_priority) | \
             ((sprio_df['PRIORITY'] == this_priority) & (sprio_df['JOBID'] < pending_job['JOBID'])))]
        problems.append(f"""This job is scheduled to run after {higher_prio_jobs.shape[0]} higher priority jobs.
   To get scheduled sooner, you can try reducing wall clock time as appropriate.""")
    if pending_job['REASON'] == 'Resources':
        severity = max(severity, SEVERITY['LOW'])
        problems.append(f"""This job is next in priority and is waiting for resources to become available on the {pending_job['PARTITION']} partition.""")
    return severity, problems

def display_queued_jobs(username, squeue):
    df = squeue.copy()
    df = df[df['USER'] == username]
    df['PARTITION (N)'] = df['PARTITION'] + ' (' + df['NODES'] + ')'
    df['REASON'] = df['REASON'].apply(color_reason)
    df['TIME'] = df['TIME'].apply(display_time)
    df = df.sort_values(by='JOBID', ascending=False)
    num_running_jobs = df[df['STATE'] == 'RUNNING'].shape[0]
    num_pending_jobs = df[df['STATE'] == 'PENDING'].shape[0]
    print(f"You have {num_running_jobs} running {'job' if num_running_jobs == 1 else 'jobs'} and {num_pending_jobs} pending {'job' if num_pending_jobs == 1 else 'jobs'} (most recent job first):")
    df['STATE'] = df['STATE'].apply(color_state)
    qos_df, sprio_df = get_qos_df(), get_sprio_df()
    df['PROBLEMS'] = df.apply(lambda pending_job: identify_problems(pending_job, squeue, qos_df, sprio_df), axis=1)
    df['JOBID'] = df.apply(display_job_id, axis=1)
    print(get_table(df[['JOBID', 'NAME', 'ACCOUNT', 'PARTITION (N)', 'QOS', 'TIME', 'STATE', 'REASON']]))

    problem_jobs = df[df['PROBLEMS'].apply(lambda p: len(p[1]) > 0)]
    for _, job in problem_jobs.iterrows():
        print('\n' + job['JOBID'] + ':')
        severity, problems = job['PROBLEMS']
        for problem in problems:
            print(' -', problem)

parser = argparse.ArgumentParser(description='Display pending job/queue info in a helpful way.')
parser.add_argument('-u', '--user', help='Set username to check (default is current user)', default=getpass.getuser())
args = parser.parse_args()

squeue = get_squeue_df()

# username = squeue[(squeue['REASON'] == 'QOSGrpNodeLimit') & (squeue['STATE'] == 'PENDING')]['USER'].values[0]
username = args.user

print(username)

if squeue[squeue['USER'] == username].shape[0] == 0: # check number of rows in current_queue
    print('You have no running or queued jobs.')
    display_recent_jobs(get_recent_jobs(username))
else:
    display_queued_jobs(username, squeue)
