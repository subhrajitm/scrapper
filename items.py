import scrapy


class WebsiteItem(scrapy.Item):
    website = scrapy.Field()
    emails = scrapy.Field()
    phones = scrapy.Field()
    vcard_links = scrapy.Field()
    vcard_files = scrapy.Field()  # Store actual vCard file content (base64 encoded)
    pdf_links = scrapy.Field()
    image_links = scrapy.Field()
    lawyer_profiles = scrapy.Field()  # List of lawyer profile data


class LawyerProfileItem(scrapy.Item):
    website = scrapy.Field()
    profile_url = scrapy.Field()
    lawyer_name = scrapy.Field()
    lawyer_email = scrapy.Field()
    lawyer_phone = scrapy.Field()
    profile_images = scrapy.Field()
    vcard_content = scrapy.Field()  # Base64 encoded vCard if found on profile
