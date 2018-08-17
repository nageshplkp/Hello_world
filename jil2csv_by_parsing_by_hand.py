# coding: utf-8
import csv


with open('example.jil', 'rt') as fl:
    code = fl.read()


def get_jobs(config_str):
    jobs, separator = [], ':'
    lines = config_str.splitlines()
    start_header = 'insert_job'
    started = False
    for line in lines:
        if not line:
            continue
        if line.strip().startswith(start_header):
            if started:
                jobs.append(job_dict)
            entry_count = line.count(separator)
            if entry_count == 0:
                continue
            if entry_count == 1:
                job_dict = dict([x.strip() for x in line.split(separator)])
            if entry_count > 1:
                entries = line.split(separator)
                inners = [y.strip() for x in entries[1:-1] for y in x.split()]
                texts = [entries[0]] + inners + [entries[-1]]
                job_dict = dict(texts[i:i + 2] for i in range(0, len(texts), 2))
            started = True
        try:
            key, value = [x.strip() for x in line.split(separator)]
        except ValueError as e:
            continue
        if started:
            job_dict[key] = value
    jobs.append(job_dict)
    return jobs

jobs = get_jobs(new_code)
# You can hardcode these if you need any specific order.
cols = set(sum((x.keys() for x in jobs), []))


with open('output.csv', 'wb') as f:
    writer = csv.DictWriter(f, cols)
    writer.writeheader()
    writer.writerows(jobs)
