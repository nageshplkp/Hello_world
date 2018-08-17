# coding: utf-8
import csv
import re
import sys


def remove_comments(string):
    # Streamed comments (/*COMMENT */)
    string = re.sub(re.compile("/\*.*?\*/",re.DOTALL ) ,"" ,string)
    # Singleline comments (//COMMENT\n )
    string = re.sub(re.compile("//.*?\n" ) ,"" ,string)
    return string


# with open('example.jil', 'rt') as fl:
#     code = fl.read()


jil_config = ''

with open(sys.argv[1], 'rt') as file_obj:
    jil_config = file_obj.read()


# Remove comments.
config = remove_comments(jil_config)


def get_jobs(config_str):
    jobs, separator = [], ':'
    lines = config_str.splitlines()
    start_header = 'insert_job'
    started = False
    job_dict = {}
    for line in lines:
        if not line:
            continue
        entry_count = line.count(separator)
        if not entry_count:
            continue
        if line.strip().startswith(start_header):
            if started:
                jobs.append(job_dict)
            job_dict = {}
            started = True
        if entry_count == 1:
            key, value = [x.strip() for x in line.split(separator)]
            job_dict.update({key: value})
        elif entry_count > 1:
            entries = line.split(separator)
            inners = [y.strip() for x in entries[1:-1] for y in x.split()]
            texts = [entries[0].strip()] + inners + [entries[-1].strip()]
            job_dict.update(dict(texts[i:i + 2] for i in range(0, len(texts), 2)))

    if job_dict:
        jobs.append(job_dict)
    return jobs

jobs = get_jobs(config)
# You can hardcode these if you need any specific order.
cols = set(sum((x.keys() for x in jobs), []))


with open('output_1.csv', 'wb') as f:
    writer = csv.DictWriter(f, cols)
    writer.writeheader()
    writer.writerows(jobs)
