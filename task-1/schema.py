cols = [
  "@type",
  "access_level",
  "bureau_code",
  "contact_point",
  "description",
  "distribution",
  "identifier",
  "issued",
  "keyword",
  "landing_page",
  "modified",
  "program_code",
  "publisher",
  "released",
  "theme",
  "title",
  "archive_exclude",
  "next_update_date"
]


schema_snake = StructType([
  StructField('@type', StringType(), True),
  StructField('access_level', StringType(), True),
  StructField('bureau_code', ArrayType(StringType(), True), True),
  StructField('contact_point', MapType(StringType(), StringType(), True), True),
  StructField('description', StringType(), True),
  StructField('distribution', ArrayType(MapType(StringType(), StringType(), True), True), True),
  StructField('identifier', StringType(), True), StructField('issued', StringType(), True),
  StructField('keyword', ArrayType(StringType(), True), True),
  StructField('landing_page', StringType(), True),
  StructField('modified', StringType(), True),
  StructField('program_code', ArrayType(StringType(), True), True),
  StructField('publisher', MapType(StringType(), StringType(), True), True),
  StructField('released', StringType(), True),
  StructField('theme', ArrayType(StringType(), True), True),
  StructField('title', StringType(), True),
  StructField('next_update_date', StringType(), True),
  StructField('archive_exclude', BooleanType(), True)
])


schema_camel = StructType([
  StructField('@type', StringType(), True),
  StructField('accessLevel', StringType(), True),
  StructField('bureauCode', ArrayType(StringType(), True), True),
  StructField('contactPoint', MapType(StringType(), StringType(), True), True),
  StructField('description', StringType(), True),
  StructField('distribution', ArrayType(MapType(StringType(), StringType(), True), True), True),
  StructField('identifier', StringType(), True), StructField('issued', StringType(), True),
  StructField('keyword', ArrayType(StringType(), True), True),
  StructField('landingPage', StringType(), True),
  StructField('modified', StringType(), True),
  StructField('programCode', ArrayType(StringType(), True), True),
  StructField('publisher', MapType(StringType(), StringType(), True), True),
  StructField('released', StringType(), True),
  StructField('theme', ArrayType(StringType(), True), True),
  StructField('title', StringType(), True),
  StructField('nextUpdateDate', StringType(), True),
  StructField('archiveExclude', BooleanType(), True)
])

