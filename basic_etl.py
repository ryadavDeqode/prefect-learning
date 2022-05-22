from prefect import task,Flow

data = [1,2,3,4]

@task
def extract(data):
    # with open(source) as f:
        # data = f.readline().strip()
        # data = data.split(',')
        return data

@task
def transform(data):
    tdata = [int(i)+1 for i in data]
    return tdata

@task
def load(data,source):
    import csv
    print('---------------')
    print(data)
    print('---------------')
    with open(source,'w') as wr:
        writer = csv.writer(wr)
        writer.writerow(data)
with Flow("etl_flow") as etl:
    data = extract(data)
    tdata = transform(data)
    load(tdata,'data.csv')


# etl.visualize()
etl.register(project_name = "etl_test")


