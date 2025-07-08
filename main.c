#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdlib.h>
#include <time.h>
#include <pthread.h>
#include <sys/stat.h>
#include <sys/ioctl.h>
#include <linux/fs.h>
#include <stdint.h>
#include <stdbool.h>
#include <stdatomic.h>
#include <assert.h>

#define NUM_THREADS 16
#define NUM_IO_BATCH 16
#define MAX_IO_SIZE (1 << 22)
#define MB2B (1ULL<<20)
#define KEY_DATA_LEN 16

typedef struct
{
	size_t io_size;
	double proportion;
} IOSizeProportion;

/************ IO CONFIG START *************/
IOSizeProportion io_sizes[] = {
	{4 * MB2B, 100.0},
};

int unmap_trim_proportion = 50; // the unmap pct in write
/************* IO CONFIG END *************/



int sequential_proportion = 0;

struct bdev_desc
{
	char *path;
	int fd;
	size_t size;
	unsigned int sector_size;
	unsigned int verify_unit_size;
	int dscdpct;
	_Atomic int pend_to_end;
	_Atomic int pend_to_start;
	_Atomic int io_runing;
};


typedef struct
{
	off_t offset;
	size_t len;
	bool dscd;
} io_desc_t;

void *p_static_data = NULL;
char *p_zero_buf = NULL;
struct bdev_desc g_bdev;
bool g_runflag = 0;
io_desc_t g_io_list[NUM_THREADS][NUM_IO_BATCH];
void *g_data_buf[NUM_THREADS];
void *g_read_buf[NUM_THREADS];
int is_valid_io_size(size_t io_size)
{
	return io_size % g_bdev.sector_size == 0;
}

int is_valid_io_proportions(IOSizeProportion *io_sizes, int num_sizes)
{
	double total_proportion = 0;
	for (int i = 0; i < num_sizes; i++)
	{
		total_proportion += io_sizes[i].proportion;
	}
	return total_proportion == 100.0;
}

int is_valid_io_type_proportions(int sequential_proportion, int unmap_trim_proportion)
{
	return (sequential_proportion >= 0 && sequential_proportion <= 100) &&
	       (unmap_trim_proportion >= 0 && unmap_trim_proportion <= 100);
}

int check_io_requirements(IOSizeProportion *io_sizes, int num_sizes, int sequential_proportion, int unmap_trim_proportion)
{
	for (int i = 0; i < num_sizes; i++)
	{
		if (!is_valid_io_size(io_sizes[i].io_size) || io_sizes[i].io_size % io_sizes[0].io_size)
		{
			printf("Invalid IO size: %lu. It must be a multiple of %lu and %u.\n", io_sizes[i].io_size, io_sizes[i].io_size, g_bdev.sector_size);
			return -1;
		}
	}

	if (!is_valid_io_proportions(io_sizes, num_sizes))
	{
		printf("Error: The sum of IO size proportions must be 100%%.\n");
		return -1;
	}

	if (!is_valid_io_type_proportions(sequential_proportion, unmap_trim_proportion))
	{
		printf("Error: Invalid proportion for sequential IO or Unmap/Trim.\n");
		return -1;
	}

	return 0; 
}

struct timespec start_time, current_time;
double total_write_count[NUM_THREADS];
double total_read_count[NUM_THREADS];
double total_discard_count[NUM_THREADS];

int blk_write_if(int fd, void *data, off_t offset, size_t len, int thdidx)
{
	ssize_t bytes_written = pwrite(fd, data, len, offset);
	if (bytes_written != (ssize_t)len)
	{
		perror("pwrite");
		assert(0);
	}
	total_write_count[thdidx]++;
	return 0;
}

int blk_read_if(int fd, void *data, off_t offset, size_t len, int thdidx)
{
	ssize_t bytes_read = pread(fd, data, len, offset);
	if (bytes_read != (ssize_t)len)
	{
		perror("pread");
		assert(0);
	}
	total_read_count[thdidx]++;
	return 0;
}

int blk_dscd_if(int fd, off_t offset, size_t len, int thdidx)
{
	
	struct blk_range
	{
		off_t start;
		size_t len;
	} range = {offset, len};

	
	if (ioctl(fd, BLKDISCARD, &range) < 0)
	{
		perror("Failed to perform TRIM/UNMAP");
		close(fd);
		return -1;
	}
	total_discard_count[thdidx]++;
	return 0;
}

void blk_write_fill_data(void *local_data_buf, off_t offset, size_t len)
{
	int unit_count = len / g_bdev.verify_unit_size;
	assert(len % g_bdev.verify_unit_size == 0);
	assert(KEY_DATA_LEN == sizeof(off_t) + sizeof(time_t));

	for (int i = 0; i < unit_count; i++)
	{
		void *data_tmp = local_data_buf + i * g_bdev.verify_unit_size;
		*(off_t *)data_tmp = offset + i * g_bdev.verify_unit_size;
		snprintf(data_tmp + sizeof(off_t), sizeof(time_t), "%ld", time(NULL)); 
	}
}



void *io_thread_func(void *arg)
{
	int thdidx = *(int *)arg;
	free(arg);
	void *local_data_buf = g_data_buf[thdidx];
	io_desc_t *local_io_list = g_io_list[thdidx];
	io_desc_t *io_tmp;

	while (true)
	{
		while (atomic_load(&g_bdev.io_runing) == 0)
		{
			usleep(1);
		}
		atomic_fetch_sub(&g_bdev.pend_to_start, 1);
		while (atomic_load(&g_bdev.pend_to_start) != 0)
		{
			usleep(1);
		}

		for (int i = 0; i < NUM_IO_BATCH; i++)
		{
			io_tmp = &local_io_list[i];
			if (io_tmp->dscd)
			{
				blk_dscd_if(g_bdev.fd, io_tmp->offset, io_tmp->len, thdidx);
			}
			else
			{
				blk_write_fill_data(local_data_buf, io_tmp->offset, io_tmp->len);
				blk_write_if(g_bdev.fd, local_data_buf, io_tmp->offset, io_tmp->len, thdidx);
			}
		}

		atomic_fetch_sub(&g_bdev.pend_to_end, 1);
		atomic_store(&g_bdev.io_runing, 0);

	}
}

void *allocate_aligned_buffer(size_t size)
{
	void *buffer;
	if (posix_memalign((void **)&buffer, g_bdev.sector_size, size) != 0)
	{
		perror("posix_memalign");
		exit(EXIT_FAILURE);
	}
	return buffer;
}

void get_current_time_string(char* buffer, size_t size) {
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    time_t t = ts.tv_sec;
    struct tm* tm_info = localtime(&t);
    strftime(buffer, size, "%Y-%m-%d %H:%M:%S", tm_info);
}

void statistics_iops(void)
{
	clock_gettime(CLOCK_MONOTONIC, &current_time);

	long elapsed_seconds = current_time.tv_sec - start_time.tv_sec;

	if (elapsed_seconds >= 1)
	{
		double write_iops = 0;
		double read_iops = 0;
		double discard_iops = 0;

		for (int i = 0; i < NUM_THREADS; i++) {
			write_iops += total_write_count[i];
			read_iops += total_read_count[i];
			discard_iops += total_discard_count[i];

			total_write_count[i] = 0;
			total_read_count[i] = 0;
			total_discard_count[i] = 0;	
		}

		write_iops = write_iops / elapsed_seconds;
		read_iops = read_iops / elapsed_seconds;
		discard_iops = discard_iops / elapsed_seconds;

		char current_time_str[100];
            	get_current_time_string(current_time_str, sizeof(current_time_str));

		printf("%s: ", current_time_str);
		printf("Total IOPS: %.0f   ", discard_iops + read_iops + write_iops);
		printf("write:%.0f ", write_iops);
		printf("read:%.0f ", read_iops);
		printf("dscd:%.0f\n", discard_iops);
		
		start_time = current_time;
	}
}


bool check_overlap(io_desc_t new_io, io_desc_t *io_list, int io_count)
{
	for (int i = 0; i < io_count; i++)
	{
		io_desc_t existing_io = io_list[i];

		
		if ((new_io.offset < existing_io.offset + (long int)existing_io.len) &&
		    (new_io.offset + (long int)new_io.len > existing_io.offset))
		{
			return true; 
		}
	}
	return false; 
}
uint32_t pick_io_size() {
    double r = (rand() % 10000) / 100.0;  

    double sum = 0;
    for (uint64_t i = 0; i < sizeof(io_sizes)/sizeof(io_sizes[0]); i++) {
        sum += io_sizes[i].proportion;
        if (r < sum) {
            return io_sizes[i].io_size;
        }
    }

    return io_sizes[sizeof(io_sizes)/sizeof(io_sizes[0]) - 1].io_size;
}


void generate_io_operations(int num_io_ops)
{
	io_desc_t io_list[num_io_ops];
	int io_count = 0;
	off_t offset;
	size_t len;
	bool dscd;

	while (io_count < num_io_ops)
	{
		
		offset = ((off_t)rand() % (g_bdev.size / g_bdev.verify_unit_size)) * g_bdev.verify_unit_size;
		
		len = pick_io_size();
		dscd = (rand() % 100) < g_bdev.dscdpct;

		
		io_desc_t new_io = {offset, len, dscd};
		
		if (!check_overlap(new_io, io_list, io_count) && offset+len <= g_bdev.size) {
			
			g_io_list[io_count / NUM_IO_BATCH][io_count % NUM_IO_BATCH] = new_io;
			io_list[io_count++] = new_io;
		}
	}
}

void io_wd_start()
{
	pthread_t threads[NUM_THREADS];
	int i, j;
	for (i = 0; i < NUM_THREADS; i++)
	{
		g_data_buf[i] = allocate_aligned_buffer(MAX_IO_SIZE);
		memcpy(g_data_buf[i], p_static_data, MAX_IO_SIZE);
		g_read_buf[i] = allocate_aligned_buffer(MAX_IO_SIZE);

		int *thdidx = malloc(sizeof(int));
		*thdidx = i;
		pthread_create(&threads[i], NULL, io_thread_func, (void *)thdidx);
	}

	clock_gettime(CLOCK_MONOTONIC, &start_time);

	while (1)
	{
		generate_io_operations(NUM_IO_BATCH * NUM_THREADS);
		atomic_store(&g_bdev.pend_to_start, NUM_THREADS);
		atomic_store(&g_bdev.pend_to_end, NUM_THREADS);
		atomic_store(&g_bdev.io_runing, 1);
		while (atomic_load(&g_bdev.pend_to_end))
		{
			usleep(1);
		}
		statistics_iops();
	}
	
	for (j = 0; j < NUM_THREADS; j++)
	{
		pthread_join(threads[j], NULL);
	}
}


void generate_compressed_data(void **data)
{
	#define COMPRESS_UNIT 4096 
	#define COMPRESSION_RATIO 0.7
	
	*data = malloc(MAX_IO_SIZE);
	if (*data == NULL)
	{
		printf("Memory allocation failed\n");
		return;
	}

	srand(time(NULL));

	for (size_t block = 0; block < MAX_IO_SIZE / COMPRESS_UNIT; block++)
	{
		size_t offset = block * COMPRESS_UNIT;
		size_t valid_data_size = COMPRESS_UNIT * COMPRESSION_RATIO;
		
		for (size_t i = 0; i < valid_data_size; i++)
		{
				*(int *)(*data + i) = rand() % 256; 
		}

		for (size_t i = valid_data_size; i < COMPRESS_UNIT; i++)
		{
				*(char *)(*data + offset + i) = 0; 
		}
	}
}


int get_sector_size(const char *device_path)
{
	int fd = open(device_path, O_RDONLY);
	if (fd == -1)
	{
		perror("Failed to open block device");
		return -1;
	}

	int sector_size;
	if (ioctl(fd, BLKSSZGET, &sector_size) == -1)
	{
		perror("Failed to get block device sector size");
		close(fd);
		return -1;
	}

	close(fd);
	return sector_size;
}


int64_t get_device_size(const char *device_path)
{
	int fd = open(device_path, O_RDONLY);
	if (fd == -1)
	{
		perror("Failed to open block device");
		return -1;
	}

	int64_t size;
	if (ioctl(fd, BLKGETSIZE64, &size) == -1)
	{
		perror("Failed to get block device size");
		close(fd);
		return -1;
	}

	close(fd);
	return size; 
}


int is_valid_block_device(const char *device_path)
{
	struct stat st;
	if (stat(device_path, &st) == -1)
	{
		perror("Invalid device path");
		return 0;
	}

	if (!S_ISBLK(st.st_mode))
	{
		printf("The device path is not a block device.\n");
		return 0;
	}

	return 1;
}

int main(int argc, char *argv[])
{
	if (argc < 2)
	{
		fprintf(stderr, "Usage: %s <device_path> \n", argv[0]);
		return -1;
	}

	const char *device_path = argv[1];

	
	if (!is_valid_block_device(device_path))
	{
		return -1;
	}
	
	int sector_size = get_sector_size(device_path);
	if (sector_size == -1)
	{
		return -1;
	}
	printf("Block device sector size: %d bytes\n", sector_size);
	
	int64_t device_size = get_device_size(device_path);
	if (device_size == -1)
	{
		return -1;
	}
	printf("Block device total size: %lu bytes\n", device_size);

	g_bdev.path = (char *)device_path;
	g_bdev.fd = open(g_bdev.path, O_RDWR | O_DIRECT);
	g_bdev.sector_size = sector_size;
	g_bdev.size = device_size;

	if (check_io_requirements(io_sizes, sizeof(io_sizes) / sizeof(io_sizes[0]), sequential_proportion, unmap_trim_proportion) != 0)
	{
		return -1;
	}
	g_bdev.verify_unit_size = io_sizes[0].io_size;
	g_bdev.dscdpct = unmap_trim_proportion;

	generate_compressed_data(&p_static_data);
	p_zero_buf = calloc(1, MAX_IO_SIZE);

	printf("Init finish, %s start test...\n", g_bdev.path);

	io_wd_start();

	return 0;
}

