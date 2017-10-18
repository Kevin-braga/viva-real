import scrapy, json , csv , requests , math , string
from requests import Session
from requests.auth import HTTPBasicAuth
import zeep
from zeep.transports import Transport


class viva_realSpider(scrapy.Spider):
		name = "viva_real"
		allowed_domains = ["api.vivareal.com"]

		directory = r"BASE_VIVAREAL.csv"
		apiKey = "183d98b9-fc81-4ef1-b841-7432c610b36e"
		
		def start_requests(self):

			headers = ["price", "priceValue","propertyId", "siteUrl", "area", "areaUnit", "countryName", "neighborhoodName", "cityName", "stateName","zoneName", "currencySymbol", "bathrooms", "rooms", "garages", "latitude", "longitude", "thumbnails", "isDevelopmentUnit", 
			"saved", "listingType", "stateNormalized", "geolocationPrecision","externalId", "propertyTypeName", "propertyTypeId","title", "legend", "countryUrl", "neighborhoodUrl", "cityUrl", "stateUrl", "zoneUrl" , "accountName", "accountLogo", "accountRole", 
			"accountLicenseNumber", "account", "email", "leadEmails", "contactName", "contactLogo", "contactPhoneNumber", "contactCellPhoneNumber", "contactAddress", "usageId", "usageName", "businessId", "businessName", "publicationType", "positioning", "salePrice", 
			"baseSalePrice", "rentPrice", "baseRentPrice", "currency", "numImages", "image", "thumbnail", "showAddress", "address", "zipCode", "locationId", "images" , "backgroundImage", "video", "constructionStatus", "rentPeriodId", "rentPeriod", "suites", "condominiumPrice", 
			"iptu", "additionalFeatures", "developmentInformation", "creationDate",  "promotions", "geoDistance", "isFeatured", "streetId", "streetName", "streetNumber", "accountPagePath", "links"]

			with open(self.directory, "a+") as file:  
					wr = csv.writer(file)
					endline = ';'.join(headers)
					file.write(endline)
					file.write('\n')

			
			urls = [
			"https://api.vivareal.com/api/1.0/locations/listings?apiKey={0}&locationIds=BR>Acre&page={1}&business=VENTA",
			"https://api.vivareal.com/api/1.0/locations/listings?apiKey={0}&locationIds=BR%3EAlagoas&page={1}&business=VENTA",
			"https://api.vivareal.com/api/1.0/locations/listings?apiKey={0}&locationIds=BR%3EAmazonas&page={1}&business=VENTA",
			"https://api.vivareal.com/api/1.0/locations/listings?apiKey={0}&locationIds=BR%3EAmapa&page={1}&business=VENTA",
			"https://api.vivareal.com/api/1.0/locations/listings?apiKey={0}&locationIds=BR%3EBahia&page={1}&business=VENTA",
			"https://api.vivareal.com/api/1.0/locations/listings?apiKey={0}&locationIds=BR%3ECeara&page={1}&business=VENTA",
			"https://api.vivareal.com/api/1.0/locations/listings?apiKey={0}&locationIds=BR%3EDistrito%20Federal&page={1}&business=VENTA",
			"https://api.vivareal.com/api/1.0/locations/listings?apiKey={0}&locationIds=BR%3EEspirito%20Santo&page={1}&business=VENTA",
			"https://api.vivareal.com/api/1.0/locations/listings?apiKey={0}&locationIds=BR%3EGoias&page={1}&business=VENTA",
			"https://api.vivareal.com/api/1.0/locations/listings?apiKey={0}&locationIds=BR%3EMaranhao&page={1}&business=VENTA",
			"https://api.vivareal.com/api/1.0/locations/listings?apiKey={0}&locationIds=BR%3EMinas%20Gerais&page={1}&business=VENTA&areaUpTo=75",
			"https://api.vivareal.com/api/1.0/locations/listings?apiKey={0}&locationIds=BR%3EMinas%20Gerais&page={1}&business=VENTA&areaFrom=75&areaUpTo=130",
			"https://api.vivareal.com/api/1.0/locations/listings?apiKey={0}&locationIds=BR%3EMinas%20Gerais&page={1}&business=VENTA&areaFrom=131&areaUpTo=350",
			"https://api.vivareal.com/api/1.0/locations/listings?apiKey={0}&locationIds=BR%3EMinas%20Gerais&page={1}&business=VENTA&areaFrom=351",
			"https://api.vivareal.com/api/1.0/locations/listings?apiKey={0}&locationIds=BR%3EMato%20Grosso%20do%20Sul&page={1}&business=VENTA",
			"https://api.vivareal.com/api/1.0/locations/listings?apiKey={0}&locationIds=BR%3EMato%20Grosso&page={1}&business=VENTA",
			"https://api.vivareal.com/api/1.0/locations/listings?apiKey={0}&locationIds=BR%3EPara&page={1}&business=VENTA",
			"https://api.vivareal.com/api/1.0/locations/listings?apiKey={0}&locationIds=BR%3EParaiba&page={1}&business=VENTA",
			"https://api.vivareal.com/api/1.0/locations/listings?apiKey={0}&locationIds=BR%3EPernambuco&page={1}&business=VENTA",
			"https://api.vivareal.com/api/1.0/locations/listings?apiKey={0}&locationIds=BR%3EPiaui&page={1}&business=VENTA",
			"https://api.vivareal.com/api/1.0/locations/listings?apiKey={0}&locationIds=BR%3EParana&page={1}&business=VENTA",
			"https://api.vivareal.com/api/1.0/locations/listings?apiKey={0}&locationIds=BR%3ERio%20de%20Janeiro&page={1}&business=VENTA&areaUpTo=78",
			"https://api.vivareal.com/api/1.0/locations/listings?apiKey={0}&locationIds=BR%3ERio%20de%20Janeiro&page={1}&business=VENTA&areaFrom=79&areaUpTo=150",
			"https://api.vivareal.com/api/1.0/locations/listings?apiKey={0}&locationIds=BR%3ERio%20de%20Janeiro&page={1}&business=VENTA&areaFrom=151",
			"https://api.vivareal.com/api/1.0/locations/listings?apiKey={0}&locationIds=BR%3ERio%20Grande%20do%20Norte&page={1}&business=VENTA",
			"https://api.vivareal.com/api/1.0/locations/listings?apiKey={0}&locationIds=BR%3ERondonia&page={1}&business=VENTA",
			"https://api.vivareal.com/api/1.0/locations/listings?apiKey={0}&locationIds=BR%3ERoraima&page={1}&business=VENTA",
			"https://api.vivareal.com/api/1.0/locations/listings?apiKey={0}&locationIds=BR%3ERio%20Grande%20do%20Sul&page={1}&business=VENTA&areaUpTo=60",
			"https://api.vivareal.com/api/1.0/locations/listings?apiKey={0}&locationIds=BR%3ERio%20Grande%20do%20Sul&page={1}&business=VENTA&areaFrom=61&areaUpTo=90",	
			"https://api.vivareal.com/api/1.0/locations/listings?apiKey={0}&locationIds=BR%3ERio%20Grande%20do%20Sul&page={1}&business=VENTA&areaFrom=91&areaUpTo=160",
			"https://api.vivareal.com/api/1.0/locations/listings?apiKey={0}&locationIds=BR%3ERio%20Grande%20do%20Sul&page={1}&business=VENTA&areaFrom=161&areaUpTo=300",
			"https://api.vivareal.com/api/1.0/locations/listings?apiKey={0}&locationIds=BR%3ERio%20Grande%20do%20Sul&page={1}&business=VENTA&areaFrom=301",
			"https://api.vivareal.com/api/1.0/locations/listings?apiKey={0}&locationIds=BR%3ESanta%20Catarina&page={1}&business=VENTA&areaUpTo=120",
			"https://api.vivareal.com/api/1.0/locations/listings?apiKey={0}&locationIds=BR%3ESanta%20Catarina&page={1}&business=VENTA&areaFrom=121",
			"https://api.vivareal.com/api/1.0/locations/listings?apiKey={0}&locationIds=BR%3ESergipe&page={1}&business=VENTA",
			"https://api.vivareal.com/api/1.0/locations/listings?apiKey={0}&locationIds=BR%3ESao%20Paulo&page={1}&business=VENTA&areaUpTo=40",	
			"https://api.vivareal.com/api/1.0/locations/listings?apiKey={0}&locationIds=BR%3ESao%20Paulo&page={1}&business=VENTA&areaFrom=41&areaUpTo=47",
			"https://api.vivareal.com/api/1.0/locations/listings?apiKey={0}&locationIds=BR%3ESao%20Paulo&page={1}&business=VENTA&areaFrom=48&areaUpTo=50",	
			"https://api.vivareal.com/api/1.0/locations/listings?apiKey={0}&locationIds=BR%3ESao%20Paulo&page={1}&business=VENTA&areaFrom=51&areaUpTo=54",	
			"https://api.vivareal.com/api/1.0/locations/listings?apiKey={0}&locationIds=BR%3ESao%20Paulo&page={1}&business=VENTA&areaFrom=55&areaUpTo=58",	
			"https://api.vivareal.com/api/1.0/locations/listings?apiKey={0}&locationIds=BR%3ESao%20Paulo&page={1}&business=VENTA&areaFrom=59&areaUpTo=62",	
			"https://api.vivareal.com/api/1.0/locations/listings?apiKey={0}&locationIds=BR%3ESao%20Paulo&page={1}&business=VENTA&areaFrom=63&areaUpTo=66",	
			"https://api.vivareal.com/api/1.0/locations/listings?apiKey={0}&locationIds=BR%3ESao%20Paulo&page={1}&business=VENTA&areaFrom=67&areaUpTo=69",	
			"https://api.vivareal.com/api/1.0/locations/listings?apiKey={0}&locationIds=BR%3ESao%20Paulo&page={1}&business=VENTA&areaFrom=70&areaUpTo=72",	
			"https://api.vivareal.com/api/1.0/locations/listings?apiKey={0}&locationIds=BR%3ESao%20Paulo&page={1}&business=VENTA&areaFrom=73&areaUpTo=77",	
			"https://api.vivareal.com/api/1.0/locations/listings?apiKey={0}&locationIds=BR%3ESao%20Paulo&page={1}&business=VENTA&areaFrom=78&areaUpTo=82",	
			"https://api.vivareal.com/api/1.0/locations/listings?apiKey={0}&locationIds=BR%3ESao%20Paulo&page={1}&business=VENTA&areaFrom=83&areaUpTo=89",	
			"https://api.vivareal.com/api/1.0/locations/listings?apiKey={0}&locationIds=BR%3ESao%20Paulo&page={1}&business=VENTA&areaFrom=91&areaUpTo=99",	
			"https://api.vivareal.com/api/1.0/locations/listings?apiKey={0}&locationIds=BR%3ESao%20Paulo&page={1}&business=VENTA&areaFrom=100&areaUpTo=106",	
			"https://api.vivareal.com/api/1.0/locations/listings?apiKey={0}&locationIds=BR%3ESao%20Paulo&page={1}&business=VENTA&areaFrom=107&areaUpTo=116",
			"https://api.vivareal.com/api/1.0/locations/listings?apiKey={0}&locationIds=BR%3ESao%20Paulo&page={1}&business=VENTA&areaFrom=117&areaUpTo=125",
			"https://api.vivareal.com/api/1.0/locations/listings?apiKey={0}&locationIds=BR%3ESao%20Paulo&page={1}&business=VENTA&areaFrom=126&areaUpTo=137",
			"https://api.vivareal.com/api/1.0/locations/listings?apiKey={0}&locationIds=BR%3ESao%20Paulo&page={1}&business=VENTA&areaFrom=138&areaUpTo=149",
			"https://api.vivareal.com/api/1.0/locations/listings?apiKey={0}&locationIds=BR%3ESao%20Paulo&page={1}&business=VENTA&areaFrom=150&areaUpTo=159",
			"https://api.vivareal.com/api/1.0/locations/listings?apiKey={0}&locationIds=BR%3ESao%20Paulo&page={1}&business=VENTA&areaFrom=160&areaUpTo=174",
			"https://api.vivareal.com/api/1.0/locations/listings?apiKey={0}&locationIds=BR%3ESao%20Paulo&page={1}&business=VENTA&areaFrom=175&areaUpTo=193",
			"https://api.vivareal.com/api/1.0/locations/listings?apiKey={0}&locationIds=BR%3ESao%20Paulo&page={1}&business=VENTA&areaFrom=194&areaUpTo=215",
			"https://api.vivareal.com/api/1.0/locations/listings?apiKey={0}&locationIds=BR%3ESao%20Paulo&page={1}&business=VENTA&areaFrom=216&areaUpTo=246",
			"https://api.vivareal.com/api/1.0/locations/listings?apiKey={0}&locationIds=BR%3ESao%20Paulo&page={1}&business=VENTA&areaFrom=247&areaUpTo=276",
			"https://api.vivareal.com/api/1.0/locations/listings?apiKey={0}&locationIds=BR%3ESao%20Paulo&page={1}&business=VENTA&areaFrom=277&areaUpTo=315",
			"https://api.vivareal.com/api/1.0/locations/listings?apiKey={0}&locationIds=BR%3ESao%20Paulo&page={1}&business=VENTA&areaFrom=316&areaUpTo=375",
			"https://api.vivareal.com/api/1.0/locations/listings?apiKey={0}&locationIds=BR%3ESao%20Paulo&page={1}&business=VENTA&areaFrom=376&areaUpTo=477",
			"https://api.vivareal.com/api/1.0/locations/listings?apiKey={0}&locationIds=BR%3ESao%20Paulo&page={1}&business=VENTA&areaFrom=488&areaUpTo=700",
			"https://api.vivareal.com/api/1.0/locations/listings?apiKey={0}&locationIds=BR%3ESao%20Paulo&page={1}&business=VENTA&areaFrom=701&areaUpTo=1500",
			"https://api.vivareal.com/api/1.0/locations/listings?apiKey={0}&locationIds=BR%3ESao%20Paulo&page={1}&business=VENTA&areaFrom=1501",
			"https://api.vivareal.com/api/1.0/locations/listings?apiKey={0}&locationIds=BR%3ETocantins&page={1}&business=VENTA"
			]

			for url in urls:

				r = requests.get(url.format(self.apiKey,1))
				data = json.loads(r.text)
				total = (data['listingsCount'])

				pages = math.ceil(int(total)/20)

				for i in range(pages):

					yield scrapy.Request(url=url.format(self.apiKey,i+1), callback=self.parse)
					self.log('Saved page %s'%(i+1))
							
					   
		def parse(self, response):
				
			r = json.loads(response.body_as_unicode())

			anuncios = r['listings']

			for anuncio in anuncios:

				price = anuncio.get('price')
				priceValue = anuncio.get('priceValue')
				propertyId = anuncio.get('propertyId')
				siteUrl = anuncio.get('siteUrl')
				area = anuncio.get('area')
				areaUnit = anuncio.get('areaUnit')
				countryName = anuncio.get('countryName')
				neighborhoodName = anuncio.get('neighborhoodName')
				cityName = anuncio.get('cityName')
				stateName= anuncio.get('stateName')
				zoneName = anuncio.get('zoneName')
				currencySymbol = anuncio.get('currencySymbol')
				bathrooms = anuncio.get('bathrooms')
				rooms = anuncio.get('rooms')
				garages = anuncio.get('garages')
				latitude = anuncio.get('latitude')
				longitude = anuncio.get('longitude')
				thumbnails= anuncio.get('thumbnails')
				isDevelopmentUnit = anuncio.get('isDevelopmentUnit')
				saved = anuncio.get('saved')
				listingType = anuncio.get('listingType')
				stateNormalized = anuncio.get('stateNormalized ')
				geolocationPrecision= anuncio.get('geolocationPrecision')
				externalId = anuncio.get('externalId')
				propertyTypeName = anuncio.get('propertyTypeName')
				propertyTypeId= anuncio.get('propertyTypeId')
				title = anuncio.get('title')
				legend = anuncio.get('legend')
				countryUrl = anuncio.get('countryUrl')
				neighborhoodUrl = anuncio.get('neighborhoodUrl')
				cityUrl = anuncio.get('cityUrl')
				stateUrl = anuncio.get('stateUrl')
				zoneUrl  = anuncio.get('zoneUrl')
				accountName = anuncio.get('accountName')
				accountLogo = anuncio.get('accountLogo')
				accountRole = anuncio.get('accountRole')
				accountLicenseNumber = anuncio.get('accountLicenseNumber')
				account = anuncio.get('account')
				email = anuncio.get('email')
				leadEmails = anuncio.get('leadEmails')
				contactName = anuncio.get('contactName')
				contactLogo = anuncio.get('contactLogo')
				contactPhoneNumber = anuncio.get('contactPhoneNumber')
				contactCellPhoneNumber = anuncio.get('contactCellPhoneNumber')
				contactAddress = anuncio.get('contactAddress')
				usageId = anuncio.get('usageId')
				usageName = anuncio.get('usageName')
				businessId = anuncio.get('businessId')
				businessName = anuncio.get('businessName')
				publicationType = anuncio.get('publicationType')
				positioning = anuncio.get('positioning')
				salePrice = anuncio.get('salePrice')
				baseSalePrice = anuncio.get('baseSalePrice')
				rentPrice = anuncio.get('rentPrice')
				baseRentPrice = anuncio.get('baseRentPrice')
				currency = anuncio.get('currency')
				numImages = anuncio.get('numImages')
				image = anuncio.get('image')
				thumbnail = anuncio.get('thumbnail')
				showAddress = anuncio.get('showAddress')
				address = anuncio.get('address')
				zipCode = anuncio.get('zipCode')
				locationId = anuncio.get('locationId')
				images= anuncio.get('images')
				backgroundImage = anuncio.get('backgroundImage')
				video = anuncio.get('video')
				constructionStatus = anuncio.get('constructionStatus')
				rentPeriodId = anuncio.get('rentPeriodId')
				rentPeriod = anuncio.get('rentPeriod')
				suites = anuncio.get('suites')
				condominiumPrice = anuncio.get('condominiumPrice')
				iptu = anuncio.get('iptu')
				additionalFeatures= anuncio.get('additionalFeatures')
				developmentInformation = anuncio.get('developmentInformation')
				creationDate = anuncio.get('creationDate')
				promotions= anuncio.get('promotions')
				geoDistance = anuncio.get('geoDistance')
				isFeatured = anuncio.get('isFeatured')
				streetId = anuncio.get('streetId')
				streetName = anuncio.get('streetName')
				streetNumber = anuncio.get('streetNumber')
				accountPagePath = anuncio.get('accountPagePath')
				links= anuncio.get('links')

				dados = []

				mask_dados = [str(price),str(priceValue),str(propertyId),str(siteUrl),str(area),str(areaUnit),str(countryName),str(neighborhoodName),str(cityName),str(stateName),str(zoneName),str(currencySymbol),str(bathrooms),str(rooms),
				str(garages),str(latitude),str(longitude),str(thumbnails),str(isDevelopmentUnit),str(saved),str(listingType),str(stateNormalized),str(geolocationPrecision),str(externalId),str(propertyTypeName),str(propertyTypeId),str(title),
				str(legend),str(countryUrl),str(neighborhoodUrl),str(cityUrl),str(stateUrl),str(zoneUrl),str(accountName),str(accountLogo),str(accountRole),str(accountLicenseNumber),str(account),str(email),str(leadEmails),str(contactName),
				str(contactLogo),str(contactPhoneNumber),str(contactCellPhoneNumber),str(contactAddress),str(usageId),str(usageName),str(businessId),str(businessName),str(publicationType),str(positioning),str(salePrice),str(baseSalePrice),
				str(rentPrice),str(baseRentPrice),str(currency),str(numImages),str(image),str(thumbnail),str(showAddress),str(address),str(zipCode),str(locationId),str(images),str(backgroundImage),str(video),str(constructionStatus),str(rentPeriodId),
				str(rentPeriod),str(suites),str(condominiumPrice),str(iptu),str(additionalFeatures),str(developmentInformation),str(creationDate),str(promotions),str(geoDistance),str(isFeatured),str(streetId),str(streetName),str(streetNumber),
				str(accountPagePath),str(links)]

				for line in mask_dados:
					line = line.replace('\n','').replace('\r','').replace('\t','').replace(';','').strip()
					dados.append(line)

				with open(self.directory, "a+", encoding='utf-8') as file:  
					
						wr = csv.writer(file)
						endline = ';'.join(dados)
						file.write(endline)
						file.write('\n')

						


		def xml():

			session = Session()
			session.auth = HTTPBasicAuth('?', '?')
			transport_with_basic_auth = Transport(session=session)

			client = zeep.Client(
				wsdl='?',
				transport=transport_with_basic_auth
			)

			response = client.service.insert(
				u_cpf_cnpj = documento,
				u_tipo_pessoa = tipo_documento,
				uf = uf,
				u_cartorio_cenprot = cartorio,
				u_comarca = comarca,
				u_critica_cenprot = "Sucesso"
			)

			logger.debug("<Insert: Registro inserido no ServiceNow [%s]>"%documento)
