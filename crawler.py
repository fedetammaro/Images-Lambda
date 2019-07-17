from icrawler.builtin import GoogleImageCrawler, BingImageCrawler
import os
import sys

query = sys.argv[1]
num_images = sys.argv[2]

base_path = os.path.dirname(os.path.realpath(__file__))

google_crawler = GoogleImageCrawler(storage={'root_dir': base_path + '/images/' + query})
google_crawler.crawl(keyword=query, language='us', max_num=int(num_images))
bing_crawler = BingImageCrawler(storage={'root_dir': base_path + '/images/' + query})
bing_crawler.crawl(keyword=query, max_num=int(num_images), file_idx_offset='auto')
