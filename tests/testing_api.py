# -*- coding: utf-8 -*-

# System
import zipfile
import os
import datetime
import errno
import logging

# Third Party
import requests

API_TOURNAMENT_URL = 'https://api-tournament.numer.ai'


class NumerAPI():
    """Wrapper around the Numerai API"""

    def __init__(self, public_id=None, secret_key=None, verbosity="INFO"):
        """
        initialize Numerai API wrapper for Python

        public_id: first part of your token generated at
                   Numer.ai->Account->Custom API keys
        secret_key: second part of your token generated at
                    Numer.ai->Account->Custom API keys
        verbosity: indicates what level of messages should be displayed
            valid values: "debug", "info", "warning", "error", "critical"
        """
        if public_id and secret_key:
            self.token = (public_id, secret_key)
        elif not public_id and not secret_key:
            self.token = None
        else:
            print("You need to supply both a public id and a secret key.")
            self.token = None

        self.logger = logging.getLogger(__name__)

        # set up logging
        numeric_log_level = getattr(logging, verbosity.upper())
        if not isinstance(numeric_log_level, int):
            raise ValueError('invalid verbosity: %s' % verbosity)
        log_format = "%(asctime)s %(levelname)s %(name)s: %(message)s"
        logging.basicConfig(format=log_format, level=numeric_log_level)
        self.submission_id = None

    def _unzip_file(self, src_path, dest_path, filename):
        """unzips file located at src_path into destination_path"""
        self.logger.info("unzipping file...")

        # construct full path (including file name) for unzipping
        unzip_path = os.path.join(dest_path, filename)

        # create parent directory for unzipped data
        try:
            os.makedirs(unzip_path)
        except OSError as exception:
            if exception.errno != errno.EEXIST:
                raise

        # extract data
        with zipfile.ZipFile(src_path, "r") as z:
            z.extractall(unzip_path)

        return True

    def download_current_dataset(self,
                                 dest_path=".",
                                 dest_filename=None,
                                 unzip=True):
        """download dataset for current round

        dest_path: desired location of dataset file (optional)
        dest_filename: desired filename of dataset file (optional)
        unzip: indicates whether to unzip dataset
        """
        self.logger.info("downloading current dataset...")

        # set up download path
        if dest_filename is None:
            now = datetime.datetime.now().strftime("%Y%m%d")
            dest_filename = "numerai_dataset_{0}.zip".format(now)
        else:
            # ensure it ends with ".zip"
            if not dest_filename.endswith(".zip"):
                dest_filename += ".zip"
        dataset_path = os.path.join(dest_path, dest_filename)

        if os.path.exists(dataset_path):
            self.logger.info("target file already exists")
            return dataset_path

        # get link to current dataset (hardcode tournament 1 for now)
        query = "query {dataset(tournament: 1)}"
        url = self.raw_query(query)['data']['dataset']
        # download
        dataset_res = requests.get(url, stream=True)
        dataset_res.raise_for_status()

        # create parent folder if necessary
        try:
            os.makedirs(dest_path)
        except OSError as exception:
            if exception.errno != errno.EEXIST:
                raise

        # write dataset to file
        with open(dataset_path, "wb") as f:
            for chunk in dataset_res.iter_content(1024):
                f.write(chunk)

        # unzip dataset
        if unzip:
            # remove the ".zip" in the end
            dataset_name = dest_filename[:-4]
            self._unzip_file(dataset_path, dest_path, dataset_name)

        return dataset_path

    def _handle_call_error(self, errors):
        if isinstance(errors, list):
            for error in errors:
                if "message" in error:
                    self.logger.error(error['message'])
        elif isinstance(errors, dict):
            if "detail" in errors:
                self.logger.error(errors['detail'])

    def raw_query(self, query, variables=None, authorization=False):
        """send a raw request to the Numerai's GraphQL API

        query (str): the query
        variables (dict): dict of variables
        authorization (bool): does the request require authorization
        """
        body = {'query': query, 'variables': variables}
        headers = {
            'Content-type': 'application/json',
            'Accept': 'application/json'
        }
        if authorization and self.token:
            public_id, secret_key = self.token
            headers['Authorization'] = \
                'Token {}${}'.format(public_id, secret_key)
        r = requests.post(API_TOURNAMENT_URL, json=body, headers=headers)
        result = r.json()
        if "errors" in result:
            self._handle_call_error(result['errors'])
            # fail!
            print(str(result['errors']))
            raise ValueError

        return result

    def get_leaderboard(self, round_num=0):
        """ retrieves the leaderboard for the given round

        round_num: The round you are interested in, defaults to current round.
        """
        self.logger.info("getting leaderboard for round {}".format(round_num))
        query = '''
            query($number: Int!) {
              rounds(number: $number) {
                leaderboard {
                  consistency
                  concordance {
                    pending
                    value
                  }
                  liveLogloss
                  submissionId
                  username
                  validationLogloss
                  paymentGeneral {
                    nmrAmount
                    usdAmount
                  }
                  paymentStaking {
                    nmrAmount
                    usdAmount
                  }
                  totalPayments {
                    nmrAmount
                    usdAmount
                  }
                }
              }
            }
        '''
        arguments = {'number': round_num}
        result = self.raw_query(query, arguments)
        return result['data']['rounds'][0]['leaderboard']

    def get_staking_leaderboard(self, round_num=0):
        """ retrieves the leaderboard of the staking competition for the given
        round

        round_num: The round you are interested in, defaults to current round.
        """
        self.logger.info("getting stakes for round {}".format(round_num))
        query = '''
            query($number: Int!) {
              rounds(number: $number) {
                leaderboard {
                  consistency
                  liveLogloss
                  username
                  validationLogloss
                  stake {
                    insertedAt
                    soc
                    confidence
                    value
                    txHash
                  }

                }
              }
            }
        '''
        arguments = {'number': round_num}
        result = self.raw_query(query, arguments)
        stakes = result['data']['rounds'][0]['leaderboard']
        # filter those with actual stakes
        stakes = [item for item in stakes if item["stake"]["soc"] is not None]
        return stakes

    def get_competitions(self):
        """ get information about rounds """
        self.logger.info("getting rounds...")

        query = '''
            query {
              rounds {
                number
                resolveTime
                datasetId
                openTime
                resolvedGeneral
                resolvedStaking
              }
            }
        '''
        result = self.raw_query(query)
        return result['data']['rounds']

    def get_current_round(self):
        """get information about the current active round"""
        # zero is an alias for the current round!
        query = '''
            query {
              rounds(number: 0) {
                number
              }
            }
        '''
        data = self.raw_query(query)
        round_num = data['data']['rounds'][0]["number"]
        return round_num

    def get_submission_ids(self):
        """get dict with username->submission_id mapping"""
        query = """
            query {
              rounds(number: 0) {
                leaderboard {
                  username
                  submissionId
                }
            }
        }
        """
        data = self.raw_query(query)['data']['rounds'][0]['leaderboard']
        mapping = {item['username']: item['submissionId'] for item in data}
        return mapping

    def get_user(self):
        """get all information about you! """
        query = """
          query {
            user {
              username
              banned
              assignedEthAddress
              availableNmr
              availableUsd
              email
              id
              mfaEnabled
              status
              insertedAt
              apiTokens {
                name
                public_id
                scopes
              }
            }
          }
        """
        data = self.raw_query(query, authorization=True)['data']['user']
        return data

    def get_payments(self):
        """all your payments"""
        query = """
          query {
            user {
              payments {
                nmrAmount
                round {
                  number
                  openTime
                  resolveTime
                  resolvedGeneral
                  resolvedStaking
                }
                tournament
                usdAmount
              }

            }
          }
        """
        data = self.raw_query(query, authorization=True)['data']['user']
        return data['payments']

    def get_transactions(self):
        """all deposits and withdrawals"""
        query = """
          query {
            user {

              nmrDeposits {
                from
                id
                posted
                status
                to
                txHash
                value
              }
              nmrWithdrawals {
                from
                id
                posted
                status
                to
                txHash
                value
              }
              usdWithdrawals {
                ethAmount
                confirmTime
                from
                posted
                sendTime
                status
                to
                txHash
                usdAmount
              }
            }
          }
        """
        data = self.raw_query(query, authorization=True)['data']['user']
        return data

    def get_stakes(self):
        """all your stakes"""
        query = """
          query {
            user {
              stakeTxs {
                confidence
                insertedAt
                roundNumber
                soc
                staker
                status
                txHash
                value
              }
            }
          }
        """
        data = self.raw_query(query, authorization=True)['data']['user']
        return data['stakeTxs']

    def submission_status(self, submission_id=None):
        """display submission status of the last submission associated with
        the account

        submission_id: submission of interest, defaults to the last submission
            done with the account
        """
        if submission_id is None:
            submission_id = self.submission_id

        if submission_id is None:
            raise ValueError(
                'You need to submit something first or provide a submission ID'
            )

        query = '''
            query($submission_id: String!) {
              submissions(id: $submission_id) {
                concordance {
                  pending
                  value
                }
                consistency
                validation_logloss
              }
            }
            '''
        variable = {'submission_id': submission_id}
        data = self.raw_query(query, variable, authorization=True)
        status = data['data']['submissions'][0]
        return status

    def upload_predictions(self, file_path):
        """uploads predictions from file

        file_path: CSV file with predictions that will get uploaded
        """
        self.logger.info("uploading prediction...")

        auth_query = \
            '''
            query($filename: String!) {
                submission_upload_auth(filename: $filename) {
                    filename
                    url
                }
            }
            '''
        variable = {'filename': os.path.basename(file_path)}
        submission_resp = self.raw_query(
            auth_query, variable, authorization=True)
        submission_auth = submission_resp['data']['submission_upload_auth']
        with open(file_path, 'rb') as fh:
            requests.put(submission_auth['url'], data=fh.read())
        create_query = \
            '''
            mutation($filename: String!) {
                create_submission(filename: $filename) {
                    id
                }
            }
            '''
        variables = {'filename': submission_auth['filename']}
        create = self.raw_query(create_query, variables, authorization=True)
        self.submission_id = create['data']['create_submission']['id']
        return self.submission_id

    def stake(self, confidence, value):
        """ participate in the staking competition

        confidence: your confidence (C) value
        value: amount of NMR you are willing to stake
        """

        query = '''
          mutation($code: String,
            $confidence: String!
            $password: String
            $round: Int!
            $value: String!) {
              stake(code: $code
                    confidence: $confidence
                    password: $password
                    round: $round
                    value: $value) {
                id
                status
                txHash
                value
              }
        }
        '''
        arguments = {
            'code': 'somecode',
            'confidence': str(confidence),
            'password': "somepassword",
            'round': self.get_current_round(),
            'value': str(value)
        }
        result = self.raw_query(query, arguments, authorization=True)
        return result
