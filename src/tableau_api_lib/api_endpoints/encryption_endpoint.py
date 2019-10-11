from tableau_api_lib.api_endpoints import BaseEndpoint


class EncryptionEndpoint(BaseEndpoint):
    def __init__(self,
                 ts_connection,
                 encrypt_extracts=False,
                 decrypt_extracts=False,
                 reencrypt_extracts=False):
        """
        Builds the API endpoint for setting at-rest encryption settings for the active site.
        :param class ts_connection: the Tableau Server connection object
        :param bool encrypt_extracts: True if encrypting all extracts on the active site, False by default
        :param bool decrypt_extracts: True if decrypting all extracts on the active site, False by default
        :param bool reencrypt_extracts: True if reencrypting all extracts with new keys, False by default
        """

        super().__init__(ts_connection)
        self._encrypt_extracts = encrypt_extracts
        self._decrypt_extracts = decrypt_extracts
        self._reencrypt_extracts = reencrypt_extracts
        self._validate_inputs()

    @property
    def mutually_exclusive_params(self):
        return [
            self._encrypt_extracts,
            self._decrypt_extracts,
            self._reencrypt_extracts
        ]

    @property
    def encryption_actions(self):
        return [
            'encrypt-extracts',
            'decrypt-extracts',
            'reencrypt-extracts'
        ]

    def _validate_inputs(self):
        valid = True
        if sum(self.mutually_exclusive_params) != 1:
            valid = False
        if not valid:
            self._invalid_parameter_exception()

    @property
    def base_encryption_url(self):
        return "{0}/api/{1}/sites/{2}".format(self._connection.server,
                                              self._connection.api_version,
                                              self._connection.site_id)

    @property
    def base_encryption_action_url(self):
        encryption_action = None
        if any(self.mutually_exclusive_params):
            action_index = [index for index, param in enumerate(self.mutually_exclusive_params) if param].pop()
            encryption_action = self.encryption_actions[action_index]
        else:
            self._invalid_parameter_exception()
        return "{0}/{1}".format(self.base_encryption_url,
                                encryption_action)

    def get_endpoint(self):
        return self.base_encryption_action_url
