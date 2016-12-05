# -*- coding: utf-8 -*-
"""
Defines functionality to check case existence in dbGaP.
"""

from gdcdatamodel.models import Project
from xml.parsers.expat import ExpatError

import logging
import requests
import xmltodict
import re


from .errors import (
    InternalError,
    UserError,
)

PHSID_REGEX = re.compile('(phs\d+.v)(\d)(.*)')

COMPLETE_STATE = ['released', 'completed_by_gpa']

class dbGaPXReferencer(object):

    #: The url from which to pull telemetry reports for project with given
    #: accession number (to be formatted in)
    DEFAULT_URL = ('http://www.ncbi.nlm.nih.gov/projects/gap/cgi-bin/'
                   'GetSampleStatus.cgi?study_id={phsid}&rettype=xml')

    def __init__(self, db, logger=None, proxies={}):
        """Instantiate a class to crossvalidate entity existence in dbGaP.

        """
        self._cached_telemetry_xmls = {
            # "phsid": "telemetry xml"
        }

        self.db = db
        self.proxies = proxies
        self.logger = logger or logging.getLogger("dbGapXReferencer")
        self.logger.info('Creating new dbGaP Cross Referencer')

    def request_telemetry_report(self, phsid):
        """Makes a web request to :param:`url` for the telemetry report for
        the given :param:`phsid`

        :param str phsid:
            The accession number of the project to get the telemetry XML
            report for.
        :param str url:
            *optional* Specify the dbGaP url
        :raises gdcapi.errors.InternalError:
            Will try and raise a descriptive InternalError if something
            goes wrong.
        :returns: A dict parsed from the XML

        """

        xml = self.get_xml(phsid)

        # use previous version of telemetry report
        # if current one is not released

        if xml['DbGap']['Study']['@registration_status'] not in COMPLETE_STATE:
            current_version = xml['DbGap']['Study']['@accession']
            match = PHSID_REGEX.match(current_version)
            if not match:
                raise InternalError(
                    "Unable to cross reference cases with dbGaP. "
                    "Invalid accession number {} in telemery report from dbGap"
                    .format(phsid))
            else:
                previous_version = (
                    match.group(1) + str(int(match.group(2)) - 1) +
                    match.group(3))
                xml = self.get_xml(previous_version)
                if xml['DbGap']['Study']['@registration_status'] != 'released':
                    raise InternalError(
                        "Unable to cross reference cases with dbGaP. "
                        "Last two versions of telemetry reports from dbGap "
                        "are not released")
                return xml
        else:
            return xml

    def get_xml(self, phsid):
        if phsid not in self._cached_telemetry_xmls:

            url = self.DEFAULT_URL.format(phsid=phsid)
            self.logger.info('Pulling telemetry report from {0}'.format(url))

            # Request the XML
            r = requests.get(url, proxies=self.proxies)
            if r.status_code != 200:
                msg = ("Unable to cross reference cases with dbGaP. "
                       "Either this project is not registered in dbGaP or we "
                       "were temporarily unable to communicate with dbGaP. "
                       "Please try again later.")
                self.logger.error(msg)
                raise InternalError(msg)

            # Parse the XML
            try:
                xml = xmltodict.parse(r.text)
                self._cached_telemetry_xmls[phsid] = xml

            except ExpatError as e:
                msg = ("Unable to parse dbGaP telemetry report. "
                       "Please try again later.")
                self.logger.exception(e)
                raise InternalError(msg)

        return self._cached_telemetry_xmls[phsid]

    def get_project(self, program_name, project_code):
        """Lookup the project node

        :param str program_name: the `Program.name` e.g. TCGA
        :param str project_code: the `Project.code` e.g.e BRCA
        :returns: :class:`Project` node

        """

        self.logger.info('Looking up project {0}-{1}'
                         .format(program_name, project_code))

        with self.db.session_scope():
            project = (self.db.nodes(Project)
                       .props(code=project_code)
                       .path('programs')
                       .props(name=program_name)
                       .scalar())

            if not project:
                msg = ("Unable to find project {} in database"
                       .format(project_code))
                self.logger.error(msg)
                raise InternalError(msg)

        return project

    def get_project_accession(self, project):
        """Return the project's accession number (phsid)

        :param project: the `Project` node
        :returns: String accession number

        """

        # Return the projects phsid, (default to the project's
        # program's phsid)
        return (project.dbgap_accession_number or
                project.programs[0].dbgap_accession_number)

    def get_project_dbgap_bypassed_cases(self, project):
        """Check if there is a list of bypassed cases associated with the
        project.

        :param project: the `Project` node
        :returns: :class:`Project` a list of submitter_id strings

        """

        return project.sysan.get('dbgap_bypassed_cases') or []

    def get_registered_cases(self, project):
        """Gets the submitter_id of all the cases in latest telemetry report.
        """
        phsid = self.get_project_accession(project)

        # Pull the telemetry report for the project
        telemetry = self.request_telemetry_report(phsid)

        # Parse our the sample ids
        samples = telemetry['DbGap']['Study']['SampleList']['Sample']
        return {s['@submitted_subject_id'] for s in samples}

    def case_exists(self, program_name, project_code, case_submitter_id):
        """Checks to see if case exists in latest telemetry report.

        :param str program_name: the `Program.name` e.g. TCGA
        :param str project_code: the `Project.code` e.g.e BRCA
        :returns: :class:`bool`

        :raises:
            :class:`gdcapi.errors.InternalError` if the project isn't
            found

        """

        # Lookup the project node
        project = self.get_project(program_name, project_code)
        # Check against a local bypass list
        if case_submitter_id in self.get_project_dbgap_bypassed_cases(project):
            self.logger.warning('Found case {} in local bypass list'
                                .format(case_submitter_id))
            return True

        submitter_ids = self.get_registered_cases(project)
        return case_submitter_id in submitter_ids

    def assert_project_exists(self, project_code, phsid):
        url = self.DEFAULT_URL.format(phsid=phsid)
        self.logger.info('Pulling telemetry report from {0}'.format(url))

        # Request the XML
        r = requests.get(url, proxies=self.proxies)
        if r.status_code == 400:
            msg = ("Project appears not to exist in dbGaP.")
            raise UserError(msg)

        # Parse the XML
        try:
            xml = xmltodict.parse(r.text)
        except ExpatError as e:
            msg = ("Unable to parse dbGaP telemetry report. "
                   "Please try again later.")
            self.logger.exception(e)
            raise InternalError(msg)

        study_handle = xml['DbGap']['Study']['@study_handle']
        if study_handle != project_code:
            msg = ("Project exists in dbGaP but has a differnt "
                   "'study_handle' from the 'code' you provided. "
                   "'{}' != '{}'".format(study_handle, project_code))
            raise UserError(msg)
