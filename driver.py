import os
from os import listdir
from os.path import isfile, join
import sys
import queue
from threading import Thread
import subprocess


def upload_paths(paths_content):
    with open(base_path + "/input/" + query, "w+") as input_file:
        input_file.write(paths_content)
        input_file.close()
        os.system("~/hadoop/bin/hdfs dfs -put " + base_path + "/input/" + query + " /input/" + query + "/paths")


def batch_cycle():
    flag = True

    while flag:
        subprocess.call(['java', '-jar', 'batch.jar', query, '0.005'])
        upload_paths_queue.put(1)
        if not end_batch_queue.empty():
            flag = False
        print("=====WAITING FOR FILE UPLOAD===")
        while upload_paths_queue.qsize() > 0 and flag:
            pass
    subprocess.call(['java', '-jar', 'batch.jar', query, '0.005'])


query = sys.argv[1]
images_for_batch = int(sys.argv[2])
hdfs_path = 'hdfs://localhost:9000/'

base_path = os.path.dirname(os.path.realpath(__file__))

onlyfiles = [f for f in listdir(base_path + '/images/' + query) if
             isfile(join(base_path + '/images/' + query, f))]
file_content = ""

sent_images = 0
end_batch_queue = queue.Queue()
upload_paths_queue = queue.Queue()
first_batch_start = True
batch_thread = Thread(target=batch_cycle)

os.system("~/hadoop/bin/hdfs dfs -mkdir /images")
os.system("~/hadoop/bin/hdfs dfs -mkdir /images/" + query)
os.system("~/hadoop/bin/hdfs dfs -mkdir /input")
os.system("~/hadoop/bin/hdfs dfs -mkdir /input/" + query)

speed_process = subprocess.Popen(['java', '-jar', 'speed.jar', query])

for file in onlyfiles:
    file_content += query + ',/images/' + query + '/' + file + '\n'
    os.system("~/hadoop/bin/hdfs dfs -put " + base_path + "/images/" + query + "/" + file + " /images/" + query + "/")
    sent_images += 1
    if first_batch_start and sent_images == images_for_batch:
        upload_paths(file_content)
        file_content = ""
        batch_thread.start()
        first_batch_start = False
    if upload_paths_queue.qsize() > 0:
        upload_paths(file_content)
        file_content = ""
        upload_paths_queue.get()
        print("=====FILE UPLOADED=====")

while upload_paths_queue.qsize() <= 0:
	pass
upload_paths(file_content)
end_batch_queue.put(1)
upload_paths_queue.get()
batch_thread.join()
speed_process.kill()
