import os
import re
import scrapy
import pandas as pd

def parse_page(filename):
	with open(filename, 'r') as file:
		selector = scrapy.Selector(text=file.read())

	# Get name.
	name = selector.css('div#coreInfo > h1::text').get()

	# Get all sections separately.
	journals = get_citations_for_section(selector, 'journal-articles')
	books = get_citations_for_section(selector, 'books')
	chapters = get_citations_for_section(selector, 'chapters')
	conferences = get_citations_for_section(selector, 'conferences')
	scholarly = get_citations_for_section(selector, 'scholarly-editions')
	posters = get_citations_for_section(selector, 'posters')
	others = get_citations_for_section(selector, 'others')

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
	# Others.
	for x in others:
		person_df = person_df.append({'Name': name, 'Type': 'Other', 'Citation': x}, ignore_index=True)

	# Append to global DF.
	global all_persons_df
	all_persons_df = pd.concat([all_persons_df, person_df], sort=False)
#end

def get_citations_for_section(selector, section):
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

parse_page('./christos.html')