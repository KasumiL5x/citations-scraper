import os
import re
import pandas as pd
import scrapy
from scrapy.crawler import CrawlerProcess

class CitationsSpider(scrapy.Spider):
	name = 'citations_spider'
	download_delay = 0.25

	def start_requests(self):
		page_url = 'https://www.bournemouth.ac.uk/about/our-faculties/faculty-science-technology/our-departments/department-creative-technology/department-creative-technology-staff'
		yield scrapy.Request(url=page_url, callback=self.parse_mainpage)
	#end

	def parse_mainpage(self, selector):
		# All profile links are in a bullet list.
		all_links = selector.css('a')
		# Filter out only those that have 'staffprofiles' in the url (and filter out the general link).
		all_links = [x for x in all_links if ('staffprofiles' in x.attrib['href'] and x.attrib['href'] != 'http://staffprofiles.bournemouth.ac.uk/')]

		for link in all_links:
			yield selector.follow(url=link.attrib['href'] + '#publications', callback=self.parse_person)
		#end
	#end

	def parse_person(self, selector):
		# Get name.
		name = selector.css('div#coreInfo > h1::text').get()

		# Get all sections separately.
		journals = self.get_citations_for_section(selector, 'journal-articles')
		books = self.get_citations_for_section(selector, 'books')
		chapters = self.get_citations_for_section(selector, 'chapters')
		conferences = self.get_citations_for_section(selector, 'conferences')
		scholarly = self.get_citations_for_section(selector, 'scholarly-editions')
		posters = self.get_citations_for_section(selector, 'posters')
		exhib = self.get_citations_for_section(selector, 'exhibitions')
		perf = self.get_citations_for_section(selector, 'performances')
		artefacts = self.get_citations_for_section(selector, 'artefacts')
		compositions = self.get_citations_for_section(selector, 'compositions')
		others = self.get_citations_for_section(selector, 'others')

		# Make and populate the DF.
		person_df = pd.DataFrame(columns=['Name', 'Type', 'Citation'])
		# Add all journals.
		for x in journals:
			person_df = person_df.append({'Name': name, 'Type': 'Journal Article', 'Citation': x}, ignore_index=True)
		# Books.
		for x in books:
			person_df = person_df.append({'Name': name, 'Type': 'Book', 'Citation': x}, ignore_index=True)
		# Chapters.
		for x in chapters:
			person_df = person_df.append({'Name': name, 'Type': 'Chapter', 'Citation': x}, ignore_index=True)
		# Conferences.
		for x in conferences:
			person_df = person_df.append({'Name': name, 'Type': 'Conference', 'Citation': x}, ignore_index=True)
		# Scholarly.
		for x in scholarly:
			person_df = person_df.append({'Name': name, 'Type': 'Scholarly', 'Citation': x}, ignore_index=True)
		# Posters.
		for x in posters:
			person_df = person_df.append({'Name': name, 'Type': 'Posters', 'Citation': x}, ignore_index=True)
		# Exhibitions.
		for x in exhib:
			person_df = person_df.append({'Name': name, 'Type': 'Exhibitions', 'Citation': x}, ignore_index=True)
		# Performances.
		for x in perf:
			person_df = person_df.append({'Name': name, 'Type': 'Performances', 'Citation': x}, ignore_index=True)
		# Artefacts.
		for x in artefacts:
			person_df = person_df.append({'Name': name, 'Type': 'Artefact', 'Citation': x}, ignore_index=True)
		# Compositions.
		for x in compositions:
			person_df = person_df.append({'Name': name, 'Type': 'Compositions', 'Citation': x}, ignore_index=True)
		# Others.
		for x in others:
			person_df = person_df.append({'Name': name, 'Type': 'Other', 'Citation': x}, ignore_index=True)

		# Append to global DF.
		global all_persons_df
		all_persons_df = pd.concat([all_persons_df, person_df], sort=False)
	#end

	def get_citations_for_section(self, selector, section):
		elements = selector.css(f'h3#{section} + ul > li')
		out = []

		for el in elements.css('li'):
			# Pull the citation text.
			citation_text = el.xpath("normalize-space()").extract_first()
			# Remove 'more information' if it exists alone.
			citation_text = citation_text.replace('more information', '')
			# Remove 'view in BU repository' if it exist alone.
			citation_text = citation_text.replace('view in BU repository', '')
			# Strip double spaces onwards.
			citation_text = re.sub('  +', '', citation_text)
			# Strip trailing spaces.
			citation_text = citation_text.strip()

			# Ignore now empty elements.
			if not len(citation_text):
				continue

			out.append(citation_text)
		#end
		return out
	#end
#end

# Master DF.
all_persons_df = pd.DataFrame(columns=['Name', 'Type', 'Citation'])

# Crawling.
process = CrawlerProcess()
process.crawl(CitationsSpider)
process.start()

# Write TSV and CSV.
# Write a TSV.
output_tsv = f'./output/all_persons.tsv'
all_persons_df.to_csv(output_tsv, index=False, sep='\t')

# Write a CSV.
output_csv = f'./output/all_persons.csv'
all_persons_df.to_csv(output_csv, index=False)