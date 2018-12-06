from config import ConfigSectionKey
import cloud_az
import time
import filecmp
import logging
from test_common import setup_test_files
from test_common import teardown_test_files


logger = logging.getLogger()

# Globally declare test_folder_path variable
test_folder_path = '../tmp'
readwrite_file = 'readwrite.txt'
readonly_file = 'readonly.txt'


def set_up_cloud():
	config = ConfigSectionKey('conf', 'local')
	config.load_file('../conf/azure.ini')
	cloud_connection_name = 'cloud'
	cloud = config(cloud_connection_name)
	return cloud


def test_set_up_cloud():
	cloud = set_up_cloud()
	assert cloud.account_id == 'sandboxstoragejbn'
	assert cloud.admin_objectstore == 'admin'
	assert cloud.archive_objectstore == 'archive'
	assert cloud.capture_objectstore == 'capture'
	assert cloud.platform == 'azure'
	assert cloud.sas_token != ''
	assert cloud.storage_key != ''


# Objectstore Section
def test_objectstore_put_get():
	cloud = set_up_cloud()
	objectstore = cloud_az.Objectstore(cloud.account_id, cloud)
	setup_test_files()
	objectstore.put(f'{test_folder_path}/readwrite.txt', 'readwrite.txt')
	objectstore.get('../tmp/readwrite_downloaded.txt', 'readwrite.txt')
	assert filecmp.cmp(f1=f'{test_folder_path}/readwrite.txt', f2=f'{test_folder_path}/readwrite_downloaded.txt', shallow=False)
	teardown_test_files()


def test_objectstore_delete():
	cloud = set_up_cloud()
	objectstore = cloud_az.Objectstore(cloud.account_id, cloud)
	objectstore.delete('test.txt')
	response = objectstore.get('C:/test/get/test.txt', 'test.txt')
	assert response is False


# Queue Section
def test_queue_name():
	cloud = set_up_cloud()
	queue = cloud_az.Queue(cloud.account_id, cloud)
	assert queue.queue_name == 'capture-queue'


def queue_get():
	cloud = set_up_cloud()
	queue = cloud_az.Queue(cloud.account_id, cloud)
	return queue.get()


def test_queue_get():
	cloud = set_up_cloud()
	queue = cloud_az.Queue(cloud.account_id, cloud)
	notification = queue.get()
	assert notification.id != ''


def test_queue_delete():
	cloud = set_up_cloud()
	queue = cloud_az.Queue(cloud.account_id, cloud)

	# Delete all notifications
	while True:
		time.sleep(1)
		response = queue.get()
		if response:
			notification = cloud_az.ObjectstoreNotification(response)
			queue.delete(notification)
		else:
			break

	# test the queue is empty
	#notification = queue.get()
	#print(notification)
	assert queue.get() is None


test_queue_delete()


def invalid_queue_message_put():
	cloud = set_up_cloud()
	queue = cloud_az.Queue(cloud.account_id, cloud)
	assert queue.put("Hello World") is False














