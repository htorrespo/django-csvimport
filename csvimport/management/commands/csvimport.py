# Run sql files via django#
# www.heliosfoundation.org
import os, csv, re
from datetime import datetime
import codecs
import chardet
from ...signals import imported_csv, importing_csv

from django.db import DatabaseError
from django.core.exceptions import ObjectDoesNotExist
from django.core.management.base import LabelCommand, BaseCommand
from optparse import make_option
from django.db import models
from django.contrib.contenttypes.models import ContentType

# import pdb

from django.conf import settings
CSVIMPORT_LOG = getattr(settings, 'CSVIMPORT_LOG', 'screen')
if CSVIMPORT_LOG == 'logger':
    import logging
    logger = logging.getLogger(__name__)

INTEGER = ['BigIntegerField', 'IntegerField', 'AutoField',
           'PositiveIntegerField', 'PositiveSmallIntegerField']
FLOAT = ['DecimalField', 'FloatField']
NUMERIC = INTEGER + FLOAT
BOOLEAN = ['BooleanField', 'NullBooleanField']
BOOLEAN_TRUE = [1, '1', 'Y', 'Yes', 'yes', 'True', 'true', 'T', 't']
DATEFIELD = ['DateField', 'DateTimeField']
# Note if mappings are manually specified they are of the following form ...
# MAPPINGS = "column1=shared_code,column2=org(Organisation|name),column3=description"
# statements = re.compile(r";[ \t]*$", re.M)

def save_csvimport(props=None, instance=None):
    """ To avoid circular imports do saves here """
    try:
        if not instance:
            from csvimport.models import CSVImport
            csvimp = CSVImport()
        if props:
            for key, value in props.items():
                setattr(csvimp, key, value)
        csvimp.save()
        return csvimp.id
    except:
        # Running as command line
        print 'Assumed charset = %s\n' % instance.charset
        print '###############################\n'
        for line in instance.loglist:
            if type(line) != type(''):
                for subline in line:
                    print subline
                    print
            else:
                print line
                print

class Command(LabelCommand):
    """
    Parse and map a CSV resource to a Django model.

    Notice that the doc tests are merely illustrational, and will not run
    as is.
    """

    option_list = BaseCommand.option_list + (
               make_option('--mappings', default='',
                           help='Please provide the file to import from'),
               make_option('--model', default='iisharing.Item',
                           help='Please provide the model to import to'),
               make_option('--charset', default='',
                           help='Force the charset conversion used rather than detect it')
                   )
    help = "Imports a CSV file to a model"


    def __init__(self):
        """ Set default attributes data types """
        super(Command, self).__init__()
        self.props = {}
        self.debug = False
        self.errors = []
        self.loglist = []
        self.mappings = []
        self.custom_mappings = False
        self.defaults = []
        self.app_label = ''
        self.model = ''
        self.model_name = ''
        self.fk_model = ''
        self.fk_field = None
        self.fieldmap = {}
        self.file_name = ''
        self.nameindexes = False
        self.deduplicate = True
        self.csvfile = []
        self.charset = ''
        self.unique_fields = list()
        self.unique_related_fields = list()

    def handle_label(self, label, **options):
        """ Handle the circular reference by passing the nested
            save_csvimport function
        """
        filename = label
        mappings = options.get('mappings', [])
        modelname = options.get('model', 'Item')
        charset = options.get('charset', '')
        # show_traceback = options.get('traceback', True)
        self.setup(mappings, modelname, charset, filename)
        if not hasattr(self.model, '_meta'):
            msg = 'Sorry your model could not be found please check app_label.modelname'
            try:
                print msg
            except:
                self.loglist.append(msg)
            return
        errors = self.run()
        if self.props:
            save_csvimport(self.props, self)
        self.loglist.extend(errors)
        return

    def setup(self, mappings, modelname, charset, csvfile='', defaults='',
              uploaded=None, nameindexes=False, deduplicate=True):
        """ Setup up the attributes for running the import """
        self.defaults = self.__mappings(defaults)
        
        # Retrieve the app label and model name
        if modelname.find('.') > -1:
            app_label, model = modelname.split('.')

        # Get the model itself
        self.model_name = model
        self.charset = charset
        self.app_label = app_label
        self.model = models.get_model(app_label, model)

        # Construct the field map of the main model
        for field in self.model._meta.fields:
            self.fieldmap[field.name] = field
            if field.__class__ == models.ForeignKey:
                self.fieldmap[field.name+"_id"] = field

        # If we have custom mappings, determine the format
        if mappings:
            # Test for column=name or just name list format
            if mappings.find('=') == -1:
                mappings = self.parse_header(mappings.split(','))
            self.mappings = self.__mappings(mappings)

        # Store additional settings
        self.nameindexes = bool(nameindexes)
        self.file_name = csvfile
        self.deduplicate = deduplicate

        # Retrieve file
        if uploaded:
            self.csvfile = self.__csvfile(uploaded.path)
        else:
            self.check_filesystem(csvfile)

    def check_fkey(self, key, field):
        """ Build fkey mapping via introspection of models """
        #TODO fix to find related field name rather than assume second field
        if not key.endswith('_id'):
            if field.__class__ == models.ForeignKey:
                key += '(%s|%s)' % (field.related.parent_model.__name__,
                                    field.related.parent_model._meta.fields[1].name,)
        return key

    def check_filesystem(self, csvfile):
        """ Check for files on the file system """
        if os.path.exists(csvfile):
            if os.path.isdir(csvfile):
                self.csvfile = []
                for afile in os.listdir(csvfile):
                    if afile.endswith('.csv'):
                        filepath = os.path.join(csvfile, afile)
                        try:
                            lines = self.__csvfile(filepath)
                            self.csvfile.extend(lines)
                        except:
                            pass
            else:
                self.csvfile = self.__csvfile(csvfile)
        if not getattr(self, 'csvfile', []):
            raise Exception('File %s not found' % csvfile)

    def run(self, logid=0):
        """ Run the csvimport """
        loglist = []
        importlist = []

        # If we have named indexes, assume the first
        # row of the csv is a header
        if self.nameindexes:
            indexes = self.csvfile.pop(0)
        counter = 0

        # Set the import id if present
        if logid:
            csvimportid = logid
        else:
            csvimportid = 0

        # If we are using custom mapings, retrieve any related models
        # Only one related model is currently supported
        if self.mappings:
            loglist.append('Using manually entered mapping list')

            self.custom_mappings = True

            for custom_mapping in self.mappings:
                if custom_mapping[2]: # Non-foreign keys have None here
                    fk_key = custom_mapping[2][0]
                    required_fkey = custom_mapping[1]
                    break # Assuming just one foreign key for now.  

            # Get the related model appp label, if different
            try:
                new_app_label = ContentType.objects.get(model__iexact=fk_key).app_label
            except:
                new_app_label = self.app_label

            # Store the related model itself
            self.fk_model = models.get_model(new_app_label, fk_key)

        # No custom mappings, so retrieve the mappings from the first row
        # of the csv file
        else:
            mappingstr = self.parse_header(self.csvfile[0])
            if mappingstr:
                loglist.append('Using mapping from first row of CSV file')
                self.mappings = self.__mappings(mappingstr)

        # If neither method worked, return an error
        if not self.mappings:
            loglist.append('''No fields in the CSV file match %s.%s\n
                                   - you must add a header field name row
                                   to the CSV file or supply a mapping list''' %
                                (self.model._meta.app_label, self.model.__name__))
            return loglist

        # Process each additional row in the file
        for row in self.csvfile[1:]:
            # Update the logger
            if CSVIMPORT_LOG == 'logger':
                logger.info("Import %s %i", self.model.__name__, counter)
            counter += 1

            model_instance = None
            main_model_fields = dict()
            related_model_instance = None
            related_model_fields = dict()

            # process each field in the mappings
            for (column, field, foreignkey) in self.mappings:
                field_type = self.fieldmap.get(field).get_internal_type()

                # either proceed in order or use the indexes to find
                # the right column
                if self.nameindexes:
                    column = indexes.index(column)
                else:
                    column = int(column)-1

                # Strip out unecessary spaces if needed
                try:
                    row[column] = row[column].strip()
                except AttributeError:
                    pass

                # Log this mapping
                if self.debug:
                    loglist.append('%s.%s = "%s"' % (self.model.__name__,
                                                          field, row[column]))
                # Tidy up boolean data
                if field_type in BOOLEAN:
                    row[column] = row[column] in BOOLEAN_TRUE

                # Tidy up numeric data
                if field_type in NUMERIC:
                    if not row[column]:
                        row[column] = 0
                    else:
                        try:
                            row[column] = float(row[column])
                        except:
                            loglist.append('Column %s = %s is not a number so is set to 0' \
                                                % (field, row[column]))
                            row[column] = 0
                    if field_type in INTEGER:
                        if row[column] > 9223372036854775807:
                            loglist.append('Column %s = %s more than the max integer 9223372036854775807' \
                                                % (field, row[column]))
                        if str(row[column]).lower() in ('nan', 'inf', '+inf', '-inf'):
                            loglist.append('Column %s = %s is not an integer so is set to 0' \
                                                % (field, row[column]))
                            row[column] = 0
                        row[column] = int(row[column])
                        if row[column] < 0 and field_type.startswith('Positive'):
                            loglist.append('Column %s = %s, less than zero so set to 0' \
                                                % (field, row[column]))
                            row[column] = 0

                # Tidy up date data, for now only accepting 'YYYY-MM-DD' format
                if field_type in DATEFIELD:
                    from datetime import datetime

                    try:
                        row[column] = datetime.strptime(row[column], '%Y-%m-%d')
                    except:
                        row[column] = None
                
                # Store the value in the appropriate field dictionary
                if row[column] != '':
                    if foreignkey:
                        related_model, related_field = foreignkey
                        related_model_fields[related_field] = row[column]
                        self.fk_field = field
                    else:
                        main_model_fields[field] = row[column]

            #if self.defaults:
            #    for (field, value, foreignkey) in self.defaults:
            #        try:
            #            done = model_instance.getattr(field)
            #        except:
            #            done = False
            #        if not done:
            #            if foreignkey:
            #                value = self.insert_fkey(foreignkey, value)
            #            model_instance.__setattr__(field, value)

            # Send presave signal
            importing_csv.send(sender=self.model,
                               instance=model_instance,
                               row=dict(zip(self.csvfile[:1][0], row)))

            related_model_saved = False

            # First the related model
            if self.deduplicate:
                matchdict = {}
                full_match = True
                related_model_created = False

                # Determine if we are doing a full field match
                # or only a subset of fields
                if len(self.unique_related_fields) > 0:
                    # Match on specified fields
                    full_match = False
                    for field in self.unique_related_fields:
                        matchdict[field] = related_model_fields[field]                   
                else:
                    # Match on all foreign key fields
                    for (column, field, foreignkey) in self.mappings:
                        if foreignkey:
                            matchdict[foreignkey[1]] = related_model_fields[field]

                # Retrieve model if it exists, otherwise create it
                try:
                    related_model_instance = self.fk_model.objects.get(**matchdict)
                except self.fk_model.DoesNotExist:
                    related_model_instance = self.fk_model(**related_model_fields)
                    related_model_created = True

                except self.fk_model.MultipleObjectsReturned:
                    related_model_instance = self.fk_model.objects.filter(**matchdict)[0]

                # If an existing model was found, updated it with the new data
                if not related_model_created:    
                    for field, value in related_model_fields.iteritems():
                        setattr(related_model_instance, field, value)
    
            # Not doing deduplication
            else:
                related_model_created = True
                related_model_instance = self.fk_model(**related_model_fields)
    
            # Store the import id for later and save the model
            related_model_instance.csvimport_id = csvimportid

            # Save the model instance
            try:
                related_model_instance.save()
            except DatabaseError, err:
                loglist.append('Database Error: {0}'.format(err))

            # Ensure that the foreign key field is populated with
            # the correct related_model_instance
            if self.fk_field:
                main_model_fields[self.fk_field] = related_model_instance
            else:
                raise Exception('No fk_field is set.')

            # If the related model already exists
            # get the associated main model instead of creating it
            # then update with new values
            if not related_model_created:
                query = dict()
                query[self.fk_field] = related_model_instance

                try:
                    model_instance = self.model.objects.get(**query)
                except self.model.DoesNotExist:
                    model_instance = None
                except self.model.MultipleObjectsReturned:
                    model_instance = self.model.objects.filter(**query)[0]

            # No main model was found for the existing related model instance,
            # or the related_model instance was new
            created = False
            if not model_instance:
                if self.deduplicate:
                    matchdict = {}
                    full_match = True

                    # if we have unique fields, use only those for matching,
                    # otherwise use all fields
                    if len(self.unique_fields) > 0:
                        full_match = False
                        for field in self.unique_fields:
                            try:
                                matchdict[field] = main_model_fields[field]
                            except KeyError:
                                continue
                    else: # Match on all fields
                        for (column, field, foreignkey) in self.mappings:
                            try:
                                matchdict[field] = main_model_fields[field]
                            except KeyError:
                                continue

                    try:
                        model_instance = self.model.objects.get(**matchdict)
                    except self.model.DoesNotExist:
                        created = True
                        model_instance = self.model(**main_model_fields)

                    if not created:
                        for field, value in main_model_fields.iteritems():
                            setattr(model_instance, field, value)
                else:
                    model_instance = self.model(**main_model_fields)

            model_instance.csvimport_id = csvimportid

            # Save the model            
            try:
                model_instance.save()
            except DatabaseError, err:
                loglist.append('Database Error: {0}'.format(err))

            # Send post-save signal    
            imported_csv.send(sender=self.model,
                              created=created,
                              instance=model_instance,
                              row=dict(zip(self.csvfile[:1][0], row)))

            # add pk to list if it saved properly
            if model_instance.pk:
                importlist.append(model_instance.pk)

            if CSVIMPORT_LOG == 'logger':
                for line in loglist:
                    logger.info(line)

            self.loglist.extend(loglist)
            loglist = []

        if self.loglist:
            # For some reason this is required here too
            from datetime import datetime
            self.props = {'file_name':self.file_name,
                          'import_user':'cron',
                          'upload_method':'cronjob',
                          'error_log':'\n'.join(loglist),
                          'import_date':datetime.now(),
                          'import_list':importlist}
            return self.loglist
        else:
            return ['No logging', ]

    def parse_header(self, headlist):
        """ Parse the list of headings and match with self.fieldmap """
        mapping = []
        for i, heading in enumerate(headlist):
            for key in ((heading, heading.lower(),
                         ) if heading != heading.lower() else (heading,)):
                if self.fieldmap.has_key(key):
                    field = self.fieldmap[key]
                    key = self.check_fkey(key, field)
                    mapping.append('column%s=%s' % (i+1, key))
        if mapping:
            return ','.join(mapping)
        return ''

    def error(self, message, type=1):
        """
        Types:
            0. A fatal error. The most drastic one. Will quit the program.
            1. A notice. Some minor thing is in disorder.
        """

        types = (
            ('Fatal error', FatalError),
            ('Notice', None),
        )

        self.errors.append((message, type))

        if type == 0:
            # There is nothing to do. We have to quit at this point
            raise types[0][1], message
        elif self.debug == True:
            print "%s: %s" % (types[type][0], message)

    def __csvfile(self, datafile):
        """ Detect file encoding and open appropriately """
        filehandle = open(datafile)
        if not self.charset:
            diagnose = chardet.detect(filehandle.read())
            self.charset = diagnose['encoding']
        try:
            csvfile = codecs.open(datafile, 'r', self.charset)
        except IOError:
            self.error('Could not open specified csv file, %s, or it does not exist' % datafile, 0)
        else:
            # CSV Reader returns an iterable, but as we possibly need to
            # perform list commands and since list is an acceptable iterable,
            # we'll just transform it.
            return list(self.charset_csv_reader(csv_data=csvfile,
                                                charset=self.charset))

    def charset_csv_reader(self, csv_data, dialect=csv.excel,
                           charset='utf-8', **kwargs):
        csv_reader = csv.reader(self.charset_encoder(csv_data, charset),
                                dialect=dialect, **kwargs)
        for row in csv_reader:
            # decode charset back to Unicode, cell by cell:
            yield [unicode(cell, charset) for cell in row]

    def charset_encoder(self, csv_data, charset='utf-8'):
        for line in csv_data:
            yield line.encode(charset)

    def __mappings(self, mappings):
        """
        Parse the mappings, and return a list of them.
        """
        if not mappings:
            return []

        def parse_mapping(args):
            """
            Parse the custom mapping syntax (column1=field1(ForeignKey|field),
            etc.)

            >>> parse_mapping('a=b(c|d)')
            [('a', 'b', '(c|d)')]

            * indicates that this field should be used for deduplication

            """

            pattern = re.compile(r'(\*?\w+)=(\w+)(\(\w+\|\w+\))?')
            mappings = pattern.findall(args)

            mappings = list(mappings)
            for mapping in mappings:
                mapp = mappings.index(mapping)

                mappings[mapp] = list(mappings[mapp])

                # parse foreign key component
                mappings[mapp][2] = parse_foreignkey(mapping[2])

                # * indicates that this model field should be used for
                # deduplication
                if mappings[mapp][0].startswith('*'):
                    mappings[mapp][0] = mappings[mapp][0][1:]

                    related_field = mappings[mapp][2]

                    if related_field:
                        self.unique_related_fields.append(related_field[1])
                    else:
                        self.unique_fields.append(mappings[mapp][1])

                mappings[mapp] = tuple(mappings[mapp])
            mappings = list(mappings)
            
            return mappings

        def parse_foreignkey(key):
            """
            Parse the foreignkey syntax (Key|field)

            >>> parse_foreignkey('(a|b)')
            ('a', 'b')
            """

            pattern = re.compile(r'(\w+)\|(\w+)', re.U)
            if key.startswith('(') and key.endswith(')'):
                key = key[1:-1]

            found = pattern.search(key)

            if found != None:
                return (found.group(1), found.group(2))
            else:
                return None

        mappings = mappings.replace(',', ' ')
        mappings = mappings.replace('column', '')
        return parse_mapping(mappings)


class FatalError(Exception):
    """
    Something really bad happened.
    """
    def __init__(self, value):
        self.value = value

    def __str__(self):
        return repr(self.value)

