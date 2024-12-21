from faker import Faker
import logging
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
import math

# Configure logging to display messages
logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s',
                    level=logging.INFO)


def generate_batch(batch_size):
    fake = Faker()
    return {fake.name() for _ in range(0, batch_size)}


def generate_name_parallel(size=10001, workers=4):
    chunk_size = math.ceil(size / workers)
    with ProcessPoolExecutor(max_workers=workers) as executor:
        # use executor.map
        result = executor.map(generate_batch, [chunk_size]*workers)

    return set().union(*result)


# 主函数
def main():
    """程序主入口"""
    total_names = 1000000  # 总共需要生成的名字数量
    num_workers = 1      # 并行的进程数

    logging.info(
        f"Generating {total_names} names using {num_workers} workers...")
    names = generate_name_parallel(size=total_names, workers=num_workers)

    # 输出结果
    logging.info(f"Generated {len(names)} unique names.")
    logging.info("Sample names:")
    logging.info(list(names)[:10])  # 打印前10个名字示例


if __name__ == "__main__":
    main()
