#!/bin/env python
"""
Loads delimited ascii files into Oracle with sqlldr

TO-DO:
* Might want to support input files with a header line "SKIP 1"
"""
import configparser
import os
import logging
import re
import time
import sys

from optparse import OptionParser

from loader_generic.lib.log import openlog
from loader_generic.lib.pid import PidFile


class Config:
    def __init__(self, config_file):
        self.config_file = config_file

        # Read the config file
        self.c = configparser.ConfigParser()
        self.c.read_file(open(self.config_file))

        self.devmode = self.c.getboolean('global', 'devmode')
        self.screenlog = self.c.getboolean('global', 'screenlog')

        self.base_dir = os.path.realpath(os.path.join(os.path.dirname(self.config_file), '..'))
        self.log_dir = os.path.join(self.base_dir, 'log')
        self.var_dir = os.path.join(self.base_dir, 'var')
        self.etc_dir = os.path.join(self.base_dir, 'etc')
        self.dat_dir = os.path.join(self.base_dir, 'data')
        self.sqlldr_bin = os.path.join(self.base_dir, 'venv', 'orahome', 'sqlldr')
        self.sqlldr_log_dir = os.path.join(self.base_dir, 'log')
        self.sqlldr_ctl_dir = os.path.join(self.base_dir, 'var')
        self.sqlldr_backup_dir = os.path.join(self.base_dir, 'sqlldr')
        
        # Define pid file name
        self.pidfname = os.path.join(self.var_dir, 'loader_generic.pid')
        self.pidfile = None

        # Open the log file
        self.log = openlog(
            os.path.join(self.log_dir, 'loader_generic.log'),
            stdout=self.screenlog, level=self.get_logging_lev('global')
        )
        
        # _read_databases() has to run before _read_flows()
        self.databases = {}
        self.flow_list = []
        self._read_databases()
        self._read_flows()

    def get_logging_lev(self, section):
        """
        Returns logging level for specified section
        """
        try:
            return getattr(logging, self.c.get(section, 'logging_lev').upper())
        except AttributeError:
            return logging.DEBUG

    def makePid(self, mx=1):
        """
        Create pid file
        """
        self.pidfile = PidFile(self, self.pidfname, force=self.devmode, max=mx)

    def delPid(self):
        self.pidfile.remove()

    def __getattr__(self, attr):
        return self.c.get('global', attr)

    def _read_databases(self):
        """
        Read database config sections, build Database instance.
        Must run before _read_flows() as it requires  self.db_list
        """
        for section in self.c.sections():
            fld = section.split(':')
            if len(fld) == 2 and fld[0] == 'database':
                db_name = fld[1]
                self.databases[db_name] = Database.from_config(self.c, db_name)

    def _read_flows(self):
        """
        Read flow sections from the config file:
        """
        flow_list = re.split('\s*,\s*', self.c.get('global', 'active_flows'))
        for flow_name in flow_list:
            try:
                self.flow_list.append(Flow.from_config(self.log, self.dat_dir, self.c, flow_name,  self.databases))
            except configparser.NoSectionError as e:
                self.log.warning('Flow: %s, skipping' % e)
            except configparser.ParsingError:
                # Generated when the db is not found
                pass


class Database:
    def _from_config(config, db_name):
        """
        Factory function, creates a Database instance from 
        a config object and a db name, with a config 
        record:
        
        [database:vqsd]
        user = toto
        pwd  = poipoi
        sid  = VQSD
        
        """
        sec_name = 'database:' + db_name
        return Database(db_name, config.get(sec_name, 'user'), config.get(sec_name, 'pwd'), config.get(sec_name, 'sid'))
    from_config = staticmethod(_from_config)
    
    def __init__(self, name, user, pwd, sid):
        """
        name: name of the database object
        user: username
        pwd : password
        sid : Oracle SID
        
        """
        self.name = name
        self.user = user
        self.pwd = pwd
        self.sid = sid
        
    def __str__(self):
        """
        String representation is the Oracle connect string
        """
        return '%s/%s@%s' % (self.user, self.pwd, self.sid)


class Flow:
    def _from_config(log, dat_dir, config, flow_name, database_dict):
        """
        Factory function, creates a Flow instance from 
        a config object and a flow name, with a config 
        record:
        
        [flow:some_flow]
        delimiter    = |
        input_folder = /some/path
        file_pattern = ^xdr_.+\d+$
        loadtable    = VQS_LOADTABLE_TDM
        database     = vqsd
        
        log: logging instance
        config: ConfigParser instance
        flow_name: name of a flow: [flow:<flow_name>]
        database_dict: dictionary of Database instances, key=db name
        """
        sec_name = 'flow:' + flow_name
        try:
            field_names = re.split('\s*,\s*', config.get(sec_name, 'field_names'))
            db_name = config.get(sec_name, 'database')
            return Flow(
                log=log,
                name=flow_name,
                delimiter=config.get(sec_name, 'delimiter'),
                input_folder=dat_dir,
                file_pattern=config.get(sec_name, 'file_pattern'),
                loadtable=config.get(sec_name, 'loadtable'),
                database=database_dict[db_name],
                field_names=field_names
            )
        except KeyError:
            log.warning(f'Flow: %s, no section found in config for database "%s", skipping' % (flow_name, db_name))
            raise configparser.ParsingError()
    from_config = staticmethod(_from_config)
    
    def __init__(
            self, log, name, delimiter, input_folder, file_pattern, loadtable, database, field_names, key_function=None
    ):
        """
        log : logging instance
        name: name of the flow object
        delimiter: character used to delimit fields of the input file
        input_folder: folder containing the input file(s)
        file_pattern: regex to match input file(s)
        loadtable: target table to load the input file(s)
        database: Database instance
        field_names: list of field names (=DB column names)
        key_function: passed as key argument to sort() to sort the files
        """
        self.log = log
        self.name = name
        self.delimiter = delimiter
        self.input_folder = input_folder
        self.file_pattern = re.compile(file_pattern)
        self.loadtable = loadtable
        self.database = database
        self.field_names = field_names
        self.key_function = key_function
        
        # List of files to load, populated by list_files()
        self.files = []

    def list_files(self):
        """
        Updates self.files with list of files matching self.file_pattern
        in self.input_folder.
        Sorts the generated list alphabetically or according to 
        self.key_function.
        
        """
        self.files = []
        for fname in os.listdir(self.input_folder):
            if self.file_pattern.search(fname):
                self.files.append(os.path.join(self.input_folder, fname))
    
        if self.key_function:
            self.files.sort(key=self.key_function)
        else:
            self.files.sort()
        self.log.info('%s: found %d file(s) to load' % (self.name, len(self.files)))
    
    def load(self, loader):
        """
        Loads the data of this flow using the provided loader.
        loader: Loader instance, stores all sqlldr-specific params
        """
        loader.reset()
        loader.load(
            suffix=self.name, field_names=self.field_names,
            files=self.files, database=self.database,
            loadtable=self.loadtable, delimiter=self.delimiter
        )


class LoadError(Exception):
    def __init__(self, log, level, msg):
        super(LoadError, self).__init__(msg)
        log.log(level, msg)


class LoadErrorCritical(LoadError):
    def __init__(self, log, msg):
        log.log(logging.CRITICAL, msg)


class LoadErrorWarning(LoadError):
    def __init__(self, log, msg):
        log.log(logging.WARNING, msg)


class PartialLoadError(LoadError):
    def __init__(self, log, msg):
        log.log(logging.WARNING, msg)


class Loader:
    """
    Loads a list of files into an Oracle table with sqlldr
    """
    # For UNIX, the exit codes are as follows:
    RC_UNIX = {
        0: 'EX_SUCC',
        1: 'EX_FAIL',
        2: 'EX_WARN',
        3: 'EX_FTL'
    }
    
    # For Windows NT, the exit codes are as follows:
    RC_WIN32 = {
        0: 'EX_SUCC',
        2: 'EX_WARN',
        3: 'EX_FAIL',
        4: 'EX_FTL'
    }
    
    # Text messages explaining return codes
    RC_TEXT = {
        'EX_SUCC': 'All rows loaded successfully',
        'EX_WARN': 'All or some rows rejected/discarded, or discontinued load',
        'EX_FAIL': 'Command-line or syntax errors, or Oracle errors nonrecoverable for SQL*Loader',
        'EX_FTL': 'Operating system errors (such as file open/close and malloc)'
    }
        
    def __init__(
            self, log, sqlldr_bin, sqlldr_log_dir, sqlldr_ctl_dir, sqlldr_backup_dir,
            database=None, delimiter=';', loadtable=None, sqlldr_max_error=0
    ):
        """
        log : logger instance
        sqlldr_bin : path to sqlldr executable
        sqlldr_log_dir : folder where to create sqlldr log file
        sqlldr_ctl_dir : folder where to create sqlldr control file
        sqlldr_backup_dir : folder where to save sqlldr log and bad files
        delimiter : field separatoe in the files to load
        """
        self.log = log
        self.sqlldr_bin = sqlldr_bin
        self.sqlldr_log_dir = sqlldr_log_dir
        self.sqlldr_ctl_dir = sqlldr_ctl_dir
        self.sqlldr_backup_dir = sqlldr_backup_dir
        
        self.database = database
        self.delimiter = delimiter
        self.loadtable = loadtable
    
        self.files = []
    
        self.suffix = None
        self.field_names = None
        self.sqlldr_ctl_file = None
        self.sqlldr_log_file = None
        
        self.sqlldr_max_error = sqlldr_max_error
    
    def reset(self):
        """
        Resets attributes specific to the load of one set of files
        """
        self.files = []
        self.loadtable = ''
        self.database = None
        
        self.field_names = None
        self.suffix = ''
        self.sqlldr_ctl_file = None
        self.sqlldr_log_file = None
        
    def add_file(self, fname):
        self.files.append(fname)
    
    def add_files(self, fname_list):
        self.files.extend(fname_list)
    
    def load(self, suffix, field_names, database=None, files=None, loadtable=None, delimiter=None):
        """
        suffix : to identify the load in log file and sqlldr log
        files : additional list of files to load
        database : a Database instance
        loadtable : in which table to load
        delimiter : field separatoe in the files to load
        """
        if files is None:
            files = []
        self.suffix = suffix
        self.field_names = field_names
        self.files.extend(files)
        if database:
            self.database = database
        if loadtable:
            self.loadtable = loadtable
        if delimiter:
            self.delimiter = delimiter
        
        self.sqlldr_ctl_file = os.path.join(self.sqlldr_ctl_dir, 'sqlldr.%s.ctl' % self.suffix)
        self.sqlldr_log_file = os.path.join(self.sqlldr_log_dir, 'sqlldr.%s.log' % self.suffix)
        
        self._write_ctl_file()
        self._run_sqlldr() 
    
    def _write_ctl_file(self):
        """
        Create sqlldr control file
        """
        # Build sqlldr control file
        # ctl = 'UNRECOVERABLE LOAD DATA\n'
        ctl = '''
            OPTIONS (SKIP=1)
            LOAD DATA
        '''[1:]
        for fname in self.files:
            ctl += 'INFILE "%s"\n' % fname

        ctl += """INTO TABLE %s TRUNCATE 
        FIELDS TERMINATED BY '%s' 
        TRAILING NULLCOLS\n""" % (self.loadtable, self.delimiter)

        cdr_fld = []
        for field in self.field_names:
            # Special case for dt_start and dt_end
            if field.startswith('dt_'):
                cdr_fld.append('%s TIMESTAMP "yyyy-mm-dd+hh24:mi:ss.ff3"' % field)
            elif field.lower() == 'report_date' or field.lower() == 'creation_time':
                cdr_fld.append('%s "to_date(:%s, \'YYYY/MM/DD HH24:MI:SS\')"' % (field, field))
            else:
                cdr_fld.append(field)
            
        ctl += '(%s)' % ', '.join(cdr_fld)
        
        # Write control file
        try:
            fd = open(self.sqlldr_ctl_file, 'w')
            fd.write(ctl)
            fd.close()
        except IOError as e:
            msg = '%s: Cannot write ctl file "%s": %s' % (self.suffix, self.sqlldr_ctl_file, e)
            raise LoadErrorCritical(self.log, msg)
               
    def _run_sqlldr(self):
        """
        Runs sqlldr, raises various LoadError's if problem
        """
        cmd = (
            self.sqlldr_bin,
            str(self.database),
            'direct=false',
            'log=' + self.sqlldr_log_file,
            'control=' + self.sqlldr_ctl_file,
            'errors=1000000',
            'silent=header'
        )

        start_time = time.time()

        # On Windows, before Python 2.4, must use system to get return code,
        # so use it in Unix as well to be consistent
        full_command = ' '.join(cmd)
        rc = os.system(full_command)

        if sys.platform == 'win32':
            # On Windows (cmd.exe systems), the return value is that returned 
            # by the system shell, this is the exit status of the command run
            ret_msg = Loader.RC_WIN32.get(rc, 'Unknown (%d)' % rc)
            
        else:
            # On Unix, the return value is the exit status of the process 
            # encoded in the format specified for wait()
            rc = rc >> 8
            ret_msg = Loader.RC_UNIX.get(rc, 'Unknown (%d)' % rc)
            self.log.info('sqlldr return code: %d (%s)' % (rc, ret_msg))

        load_time_sec = time.time() - start_time
        self.load_time = '%.3f' % load_time_sec

        # Look at the log file to see how it went
        info = self._sqlldr_parse_log()
        
        if not info:
            raise LoadErrorCritical(self.log, '%s: load failed: %s' % (self.suffix, ret_msg))
        
        if info['num_loaded'] == 0:
            rows_per_sec = 'n/a'
        else:
            rows_per_sec = '%.1f' % (info['num_loaded'] / load_time_sec)

        base_msg = '%s: loaded %d rows in %s sec (%s r/s) into %s' % (
            self.suffix, info['num_loaded'], self.load_time, rows_per_sec, self.loadtable
        )

        if ret_msg == 'EX_SUCC':
            # Run was ok
            msg = '%s, load successful' % base_msg
            self.log.info(msg)

        elif ret_msg == 'EX_WARN':
            # Partial load, all rejected lines written to .bad files in same 
            # folder as ctl file. If fewer then SQLLDR_MAX_ERROR error lines: 
            # warning, otherwise: critical
            #
            # In any case: remove original Geo CDR files
            if info['num_errors'] <= self.sqlldr_max_error:
                msg = '%s, partial load, discarded: %d ' % (base_msg, info['num_errors'])
                self._sqlldr_output_backup()
                raise LoadErrorWarning(self.log, msg)
            else:
                raise LoadErrorCritical(
                    self.log, '%s, partial load, too many discards: %d!' % (base_msg, info['num_errors'])
                )
        
        else:
            msg = '%s, load failed (%s)' % (base_msg, ret_msg)
            try:
                msg += ', ' + info['err_sqlldr']
            except KeyError:
                pass
            try:
                msg += ', ' + info['err_ora']
            except KeyError:
                pass
            self._sqlldr_output_backup()
            raise LoadErrorCritical(self.log, msg)
    
    def _sqlldr_output_backup(self):
        """
        Backup sqlldr.log and .bad files if they exist, for later analysis
        """
        # Prefix added to the backed-up files
        backup_prefix = self._timestamp()
        
        bad_files = [f for f in os.listdir(self.sqlldr_ctl_dir) if re.search('.+\.bad$', f)]
        for f in bad_files:
            os.rename(os.path.join(self.sqlldr_ctl_dir, f), os.path.join(self.sqlldr_backup_dir, backup_prefix + f))
        
        sql_log_bak = os.path.join(self.sqlldr_backup_dir, backup_prefix + os.path.basename(self.sqlldr_log_file))
        if os.path.isfile(self.sqlldr_log_file):
            os.rename(self.sqlldr_log_file, sql_log_bak)
    
    def _sqlldr_parse_log(self):
        """
        Parses sqlldr log file, returns a dict with possible keys:
        * num_loaded: number of loaded lines
        * num_errors: number of rows not loaded because of errors
        * err_sqlldr: sqlldr error text
        * err_ora: Oracle error text
        
        e.g. Unix:
        Total logical records skipped:          0
        Total logical records read:         73205
        Total logical records rejected:         1
        Total logical records discarded:        0
        --> returns (73205, 1)
        
        e.g. Windows:
           0 Rows successfully loaded.
        1000 Rows not loaded due to data errors.
           0 Rows not loaded because all WHEN clauses were failed.
           0 Rows not loaded because all fields were null.
        """
        try:
            fd = open(self.sqlldr_log_file)
        except IOError as e:
            self.log.warning('problem opening sqlldr log: %s' % e)
            return {}

        # E.g. of sqlldr and Oracle errors:
        # SQL*Loader-466: Column CALL_ID does not exist in table VQS_LOADTABLE_2.
        # ORA-04043: object T_REF_ITC_TKG does not exist
        
        # List of regex with named groups
        reg_list = [
            re.compile(r'^(?P<err_sqlldr>SQL\*Loader-\d+: .+)'),
            re.compile(r'^(?P<err_ora>ORA-\d+: .+)'),
            re.compile('^Total logical records read:\s*(?P<num_loaded>\d+)'),
            re.compile('^Total logical records rejected:\s*(?P<num_errors>\d+)'),
            re.compile('^\s*(?P<num_loaded>\d+)\s+Rows successfully loaded.'),
            re.compile('^\s*(?P<num_errors>\d+)\s+Rows not loaded due to data errors.')
        ]

        # Output dict default values and functions to format them
        # List of tuples: (<key name>, <default val>, <callable>)
        filters = [
            ('num_errors', 0, int),
            ('num_loaded', 0, int)
        ]
        
        # Try each regex on each line, merge matched named groups
        # into the output dictionary
        output = {'num_loaded': 0, 'num_error': 0}
        for line in fd:
            line = line.rstrip()
            for reg in reg_list:
                match = reg.search(line)
                if match:
                    output.update(match.groupdict())
                    break
        fd.close()
        
        for key, default, func in filters:
            output[key] = func(output.get(key, default))
        return output

    @staticmethod
    def _timestamp():
        """
        Returns timestamp, e.g., 20070120162316
        """
        return time.strftime('%Y%m%d%H%M%S')


def main():
    parser = OptionParser()
    parser.add_option("-c", "--config", dest='config_file', help="config file")
    (options, args) = parser.parse_args()
    if len(args) > 0:
        parser.error('Incorrect number of arguments: %d' % len(args))

    if options.config_file is None:
        parser.error('Please specify script config file (-c, --config)')

    conf = Config(options.config_file)
    conf.log.info('Starting...')
    conf.makePid()

    loader = Loader(
        conf.log, conf.sqlldr_bin, conf.sqlldr_log_dir,
        conf.sqlldr_ctl_dir, conf.sqlldr_backup_dir, conf.sqlldr_max_error
    )
    try:
        for flow in conf.flow_list:
            try:
                flow.list_files()
                flow.load(loader)
            except LoadError:
                conf.log.info('Got load error, continuing with next flow (if any)')

    finally:
        conf.delPid()
        conf.log.info('All Done!')


if __name__ == '__main__':
    main()
