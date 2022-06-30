import json
from typing import Optional, Any, Dict, List

import dataprofiler as dp
import pandas as pd
import numpy as np
import tensorflow as tf

from ruamel import yaml

import great_expectations as ge

class Rule:
    def __init__(
        self,
        domain: Dict[str, str] = None,
        parameters: Dict[str, Dict[str, Any]] = None,
        expectations: Dict[str, Dict[str, Any]] = None,
    ):
        """
            Create a new Rule to be used within the ProfilerConfigGenerator.

            Args:
                domain: a dict with a key str which acts as a field and value string 
                        which acts as a value. Used to fill in the domain_builder section
                        of a Profiler Config for this rule.
                        Required Keys:
                            - class_name: Stores the class of the domain builder, determining
                                          which domain this rule will act on
                parameters: a dict with a key str which acts as the name of the given parameter,
                            and a value dict, which contains a key str which acts as a field for
                            the given parameter and value str which acts as a value. Used to fill
                            in the parameter_builders section of a Profiler Config for this rule.

                            Note: this field could be left as an empty dict in the case that the
                            user does not need any parameters built by the RuleBasedProfiler
                
                expectations: a dict with a key str which is used to fill in the "expectation_type"
                              field for an arbitrary given expectation_configuration_builder.
                              A value dict stores a key str, which is used to represent a field
                              of the current expectation configuration and a value of type Any which
                              is used to define the value of the respective field.

                              Required Fields:
                                - class_name: Stores the class of the expectation_configuration_builder
                                  used to build this expectation. (Expected to be: "DefaultExpectationConfigurationBuilder)
                                - module_name: Stores the module to access to build the expectation
                                               (Expected to be: "great_expectations.rule_based_profiler.expectation_configuration_builder")
        """
        if domain is None:
            self.domain = {}
        else:
            self.domain = domain
        if parameters is None:
            self.parameters = {}
        else:
            self.parameters = parameters
        if expectations is None:
            self.expectations = {}
        else:
            self.expectations = expectations

    def get_domain(self):
        """
        Simple getter that returns the domain dict of this rule
        """
        return self.domain
    def set_domain(self, new_domain: Dict[str, str]):
        """
        Simple setter that sets this Rule's domain as new_domain

        Args:
            new_domain: a dict<str, str> that contains the new domain
        Returns:
            old_domain: the previous domain that was replaced
        """
        old_domain = self.domain
        self.domain = new_domain
        return old_domain

    def replace_domain_fields(self, domain_fields: Dict[str, str]):
        """
        Replaces the domain field values contained within this Rule's domain dict with the
        values provided.
        
        Note: If a domain field provided does not exist within this rules domain, it will be
        added with the provided value.

        Keys contained within this Rule's domain that are not provided within the arguments
        will be left unchanged.

        Args:
            domain_fields: dict<str, str>, contains the fields to replace and add
        Returns:
            current_domain: the updated domain for this Rule
            replaced_domain: a dict containing the keys that were replaced and 
                             their previous values
        """
        current_domain = self.domain
        replaced_domain = {}
        for field, val in domain_fields.items():
            if field in current_domain:
                replaced_domain[field] = current_domain[field]
            current_domain[field] = val
        self.domain = current_domain
        return current_domain, replaced_domain

    def delete_domain_field(self, field: str):
        """
        Deletes the provided field from this Rule's domain

        Args:
            field: the field key to remove from domain dict
        Returns:
            value: the value that was stored at the provided field and removed from the dict
                   in the case that it existed, otherwise None.
        """
        domain = self.domain
        if domain is None:
            return None
        value = domain.pop(field, None)
        self.domain = domain
        return value

    def get_parameters(self):
        """
        Simple getter that returns the parameters dict of this rule
        """
        return self.parameters
    def set_parameters(self, new_parameters: Dict[str, Dict[str, Any]]):
        """
        Simple setter that sets this Rule's parameters as new_parameters

        Args:
            new_parameters: a dict<str, dict<str,Any>> that contains the new parameters
        Returns:
            old_parameters: the previous parameters that was replaced
        """
        old_parameters = self.parameters
        self.parameters = new_parameters
        return old_parameters
    def replace_parameter_fields(self, parameter_fields: Dict[str, Dict[str,Any]]):
        """
        Replaces the parameter fields provided, for every provided parameter with the 
        provided value. 

        Note: If a parameter, or parameter field for an existing parameter, does not exist within
              this Rule's parameters, it will be added with the given value(s)
        
        Parameters or parameter fields contained within this rule that are not specified will be
        left unchanged.

        Args: 
            parameter_fields: dict<str,dict<str,Any>>, contains the parameters and respective fields
                              to be replaced or added
        Returns:
            current_parameters: this Rules updated parameters field
            replaced_parameters: a dict of the parameters and parameter field that were replaced
                                 and their previous values
        """
        current_parameters = self.parameters
        replaced_parameters = {}
        for param, fields in parameter_fields.items():
            if param in current_parameters:
                current_parameter = current_parameters[param]
                replaced_parameter = {}
                for field in fields.items():
                    if field in current_parameter:
                        replaced_parameter[field] = current_parameter[field]
                    current_parameter[field] = fields[field]
                replaced_parameters[param] = replaced_parameter
                current_parameters[param] = current_parameter
            else:
                current_parameters[param] = fields
        self.parameters = current_parameters
        return current_parameters, replaced_parameters
    
    def delete_parameter(self, name: str):
        """
        Deletes the provided parameter from the Rule's parameters dict
        
        Note: This method will remove the entire parameter. If you are interested in just deleting
              a field, see 'delete_parameter_field'

        Arg:
            name: the key of the parameter to delete
        Returns:
            value: the parameter dict that was deleted, given name was a valid key in
                   parameters, otherwise None.
        """
        parameters = self.parameters
        if parameters is None:
            return None
        value = parameters.pop(name, None)
        self.parameters = parameters
        return value

    def delete_parameter_field(self, param: str, field: str):
        """
        Deletes the provided field within the provided parameter, given that both exist as keys
        respectively.

        Args:
            param: The key to access the parameter that stores the desired field
            field: The key of the field to delete
        
        Returns:
            value: The value of the field that was removed from the parameter, on success.
                   None, otherwise.
        """
        parameters = self.parameters
        if parameters is None:
            return None
        if param in parameters:
            parameter = parameters[param]
            if parameter is None:
                return None
            value = parameter.pop(field, None)
            parameters[param] = parameter
            self.parameters = parameters
            return value
        else:
            return None

    def get_expectations(self):
        """
        Simple getter that returns the expectations dict of this rule
        """
        return self.expectations
    def set_expectations(self, new_expectations: Dict[str, Dict[str, Any]]):
        """
        Simple setter that sets this Rule's expectations as new_expectations

        Args:
            new_expectations: a dict<str, dict<str,Any>> that contains the new expectations
        Returns:
            old_expectations: the previous expectations that was replaced
        """
        old_expectations = self.expectations
        self.expectations = new_expectations
        return old_expectations
    def replace_expectation_fields(self, expectations: Dict[str, Dict[str, Any]]):
        """
        Replaces the expectation fields provided,for every provided expectation, with the 
        provided value. 

        Note: If an expectation, or expectation field for an existing expectation, does not exist within
              this Rule's expectations, it will be added with the given value(s)
        
        Expectations or expectation fields contained within this rule that are not specified will be
        left unchanged.

        Args: 
            expectations: dict<str,dict<str,Any>>, contains the expectations and respective fields
                          to be replaced or added
        Returns:
            current_expectations: this Rules updated expectations field
            replaced_expectations: a dict of the expectations and expectations field that were replaced
                                   and their previous values
        """
        current_expectations = self.expectations
        replaced_expectations = {}
        for exp, fields in expectations.items():
            if exp in current_expectations:
                current_expectation = current_expectations[exp]
                replaced_expectation = {}
                for field in fields.items():
                    if field in current_expectation:
                        replaced_expectation[field] = current_expectation[field]
                    current_expectation[field] = fields[field]
                replaced_expectations[exp] = replaced_expectation
                current_expectations[exp] = current_expectation
            else:
                current_expectations[exp] = fields
        self.expectations = current_expectations
        return current_expectations, replaced_expectations

    def delete_expectation(self, expectation: str):
        """
        Deletes the provided expectation from this Rule, given it exists

        Note: This method will delete an entire expectation. See delete_expectation_field
              for field specific deletion

        Args:
            expectation: the key to access the expectation to delete
        
        Returns:
            value: The expectation dict deleted from expectations, on success.
                   None, otherwise.
        """
        expectations = self.expectations
        if expectations is None:
            return None
        value = expectations.pop(expectation, None)
        self.expectations = expectations
        return value
    
    def delete_expectation_field(self, expectation, field):
        """
        Deletes the specified field from the provided expectation, given both exist.

        Args:
            expectation: the key used to access the expectation dict containing the desired field
            field: the key of the field to remove from the expectation

        Returns:
            value: The value of the field that was removed, on success.
                   None, otherwise.
        """
        expectations = self.expectations
        if expectations is None:
            return None
        if expectation in expectations:
            exp = expectations[expectation]
            if exp is None:
                return None
            value = exp.pop(field, None)
            expectations[expectation] = exp
            self.expectations = expectations
            return value
        else:
            return None
    
    def generate_domain_builder_config(self) -> str:
        """
        Generates the domain section for this rules config string.

        Returns:
            domain_str: the raw, multiline string containing this rules domain section
        """
        domain_fields = self.domain
        domain_str = r"""domain_builder:"""
        for key,val in domain_fields.items():
            temp_str = r"""
                """+key+r""": """+str(val)
            domain_str += temp_str
        return domain_str

    def generate_parameter_config(self, param, fields):
        """
        Generates the config for an individual parameter builder

        Args:
            param: the name of the parameter the string is being built from
            fields: the dictionary containing the names of the fields and their values
                    to be stored in the config
        Returns:
            param_str: the raw, multiline string for the given parameter builder
        """
        param_str = r"""         - name: """ + str(param) + r"""
        """
        for key, val in fields.items():
            temp_str = r"""           """+key+r""": """+json.dumps(val)+r"""
        """
            param_str += temp_str
        return param_str

    def generate_parameter_builders_config(self):
        """
        Generates the parameter builders section for this rules config string.

        Returns:
            parameters_str: the raw, multiline string containing this rules parameters section
        """
        parameters = self.parameters
        parameters_str = r"""        parameter_builders:"""
        if len(parameters.items()) > 0:
            parameters_str += r"""
        """
            for param, fields in parameters.items():
                parameters_str += self.generate_parameter_config(param, fields)
        else:
            parameters_str += r"""
    """
        return parameters_str

    def generate_expectation_config(self, expectation, params):
        """
        Generates the config for an individual expectaiton builder

        @Args:
            expectation:
                The camel_case name of the expectation being built
            params:
                The params needed in the config to build this expectation
        @Returns:
            exp_str:
                The raw, multiline expectation config string
        """
        exp_str = r"""         - expectation_type: """+ str(expectation) + r"""
        """
        for arg, val in params.items():
            param_str = r"""           """+str(arg) + r""": """ + str(val) + r"""
        """
            exp_str += param_str
        return exp_str

    def generate_expectation_builders_config(self):
        """
        Generates a string for the expectation builders section for the config of this rule

        @Returns:
            expectation_str: the raw, multiline string needed for the config to build the
                             expectation_builders section of this rule
        """
        expectations = self.expectations
        expectation_str = r"""        expectation_configuration_builders:
        """
        for exp, params in expectations.items():
            expectation_str += self.generate_expectation_config(exp, params) 
        return expectation_str

    def generate_rule_config(self):
        """
        Generates the profiler_config string for this rule.

        Returns:
            rule_str: the raw, multiline string containing the config for this rule, on success.
                      None, if missing a required field
        """
        if(
            self.domain is None or
            self.expectations is None
        ):
            return None
        domain_str = self.generate_domain_builder_config()
        parameters_str = self.generate_parameter_builders_config()
        expectations_str = self.generate_expectation_builders_config()
        rule_str = (domain_str + parameters_str + expectations_str)
        return rule_str
        

class ProfilerConfigGenerator:
    def __init__(
        self,
        name: str = "default_profiler_config_name",
        version: float = 1.0,
        variables: Dict[str, Any] = None,
        rules: Dict[str, Rule] = None,
    ):
        """
        Create a new Profiler Config Generator to Generate a RuleBasedProfiler at Runtime.

        Args:
            name: a string that contains the name of this profiler configuration
            version: a float that denotes the version of the RuleBasedProfiler to associate with this config
            variables: a dict containing a list of variables to be included for use in this config. Structure is as follows:
                    variables = {
                        variable_name: value
                    }
                    variable_name: a string containing the name of this variable, and how it will be stored in the config
                    value: Any, contains the value to store under the variable_name in the profile_config
            rules: a list of Rule objects that contain the rules to be included within this profiler_config;
                   See Rule class for documentation about the structure of a rule
        """
        self.name = name
        self.version = version
        if variables is None:
            self.variables = {}
        else:
            self.variables = variables
        if rules is None:
            self.rules = {}
        else:
            self.rules = rules
        

    def get_name(self):
        """
        Simple getter that returns the name of this ProfilerConfigGenerator object
        """
        return self.name

    def set_name(self, new_name: str):
        """
        Simple setter that updates the name of this ProfilerConfigGenerator object

        Args:
            new_name: a string that will replace the previous name
        Returns:
            old_name: the old name of this ProfilerConfigGenerator object 
        """
        old_name = self.name
        self.name = new_name
        return old_name

    def get_version(self):
        """
        Simple getter that returns the version of this ProfileConfigGenerator object
        """
        return self.version
    
    def set_version(self, new_version: float):
        """
        Simple setter that updates the version of this ProfilerConfigGenerator object

        Args:
            new_version: a flloat that will replace the previous version
        Returns:
            old_version: the old version of this ProfilerConfigGenerator object    
        """
        old_version = self.version
        self.version = new_version
        return old_version

    def get_variables(self):
        """
        Simple getter that returns the dict of variables of this ProfilerConfigGenerator object
        """
        return self.variables
    def get_variables_report(self):
        """
        Getter that returns this ProfilerConfigGenerator's variables dict as a 
        human readable report in the following format:
        
        variable_name_1: value_1
        variable_name_2: value_2
        variable_name_n: value_n
        Number of Variables: n

        Returns:
            variablesReport: string containing the human readable report
        """
        variablesReport = ""
        if self.variables is None:
            variablesReport = "ERR: Variables never set in this ProfilerConfigGenerator."
            return variablesReport
        if len(self.variables) == 0:
            variablesReport = "ERR: Variables dictionary is empty in this ProfilerConfigGenerator"
            return variablesReport
        for var, val in self.variables.items():
            variablesReport += var + r""": """ + json.dumps(val) + r"""
            """
        numVariables = len(self.variables)
        variablesReport += r"""Number of Variables: """ + str(numVariables)
        return variablesReport

    def set_variables(self, new_variables: Dict[str, Any]):
        """
        Setter that will replace this ProfilerConfigurationGenerator's dict 
        of variables with a provided dict of variables.
    
        Args:
            new_variables: Dict with str key and Any value that stores the new variables
        Returns:
            old_variables: The ProfilerConfigurationGenerator's old variables dict
        """
        old_variables = self.variables
        self.variables = new_variables
        return old_variables
    def add_variables_to_config(self, variables: Dict[str, Any]):
        """
        Adds the variables contained within the provided dict to this ProfilerConfigGenerator's
        dict of variables.
        Note: If a variable_name is provided within the dict that already exists in the 
        ProfilerConfigGenerator, the previously existing variable will be updated with the new
        value provided.

        Args:
            variables: Dict with str key and Any value that stores the variables to add
        Returns:
            currentVariables: the updated dict of variables now stored in this ProfilerConfigGenerator
            replacedVariables: a dict of all the variable_names (as keys) with values replaced by this method 
            (if any), and the previous value before replacement as the value
        """
        currentVariables = self.variables
        replacedVariables = {}
        for var, val in variables.items():
            if var in currentVariables:
                replacedVariables[var] = currentVariables[var]
            currentVariables[var] = val
        self.variables = currentVariables
        return currentVariables, replacedVariables

    def delete_variable(self, variable: str):
        """
        Deletes the specified variable from this ProfileConfigGenerator, if it exists

        Args:
            variable: the key of the variable to delete
        Returns:
            value: the value stored in the variable that was removed from this profile config,
                   on success.
                   None, otherwise.
        """
        variables = self.variables
        if variables is None:
            return None
        value = variables.pop(variable, None)
        self.variables = variables
        return value

    def get_rules(self):
        """
        Simple getter that returns the rules field of this ProfilerConfigGenerator object
        """
        return self.rules
    
    def set_rules(self, new_rules: Dict[str, Rule]):
        """
        Setter that will replace this ProfilerConfigurationGenerator's dict 
        of rules with a provided dict of rules.
    
        Args:
            new_rules: dict of Rule objects
        Returns:
            old_rules: The ProfilerConfigurationGenerator's old rules field
        """
        old_rules = self.rules
        self.rules  = new_rules
        return old_rules
    def add_rules_to_config(self, rules: Dict[str, Rule]):
        """
        Adds the rules contained within the provided dict to this ProfilerConfigGenerator's
        dict of rules.
        Note: If a rule_name is provided within the dict that already exists in the 
        ProfilerConfigGenerator, the previously existing Rule will be replaced by the new
        Rule provided.

        Args:
            rules: Dict with str key and Any value that stores the rules to add
        Returns:
            currentRules: the updated dict of rules now stored in this ProfilerConfigGenerator
            replacedRules: a dict of all the rule_names (as keys) with Rules replaced by this method 
            (if any), and the previous value before replacement as the value
        """
        currentRules = self.rules
        replacedRules = {}
        for name, rule in rules.items():
            if name in currentRules:
                replacedRules[name] = currentRules[name]
            currentRules[name] = rule
        self.rules = currentRules
        return currentRules, replacedRules
    def delete_rule(self, rule:str):
        """
        Deletes the provided rule from this ProfilerConfigGenerator, given it exists

        Note: This will delete the entire Rule from this profiler config.
              For finer grain Rule deletion, see Rule class documentation

        Args:
            rule: The key to the rule to delete from this profiler_config

        Returns:
            value: The Rule object that was deleted, on success.
                   None, otherwise.
        """
        rules = self.rules
        if rules is None:
            return None
        value = rules.pop(rule, None)
        self.rules = rules
        return value

    def generate_config_header(self) -> str:
        """
        Generates the header of a profiler_config, containing the name of this config and 
        the version

        Returns:
            header: a str containing the raw, multiline header for this profiler_config
        """
        name = self.name
        version = self.version
        header = r"""
    name: """+str(name)+r"""
    config_version: """+json.dumps(version)+r"""
    """
        return header

    def generate_config_variables(self) -> str:
        """
        Generates the variables section of a profiler_config

        Returns:
            variables_str: a str containing the raw, multiline variables section for this
                           profiler_config
        """
        variables = self.variables
        variables_str = r"""
    variables:
        """
        for var, val in variables.items():
            temp_str = str(var) + r""": """ + json.dumps(val) + r"""
        """
            variables_str += temp_str
        variables_str += r"""
    """
        return variables_str

    def generate_config_rules(self) -> str:
        """
        Generates the rules section of a profiler_config

        Returns:
            rules_str: a str containing the raw, multiline rules section for this profiler_config
        """
        rules = self.rules
        rules_str = r"""rules:
        """
        for name, rule in rules.items():
            rule_name = name+r""":
            """
            rule_body = rule.generate_rule_config()
            if rule_body is None: #Param checking rules provided
                return None
            rule_str = rule_name + rule_body
            rules_str += rule_str
        return rules_str

    def generate_profiler_config(self):
        """
        Generates a profiler_config string based on this objects fields.

        Returns:
            profiler_config: a raw, multiline string consisting of the profiler_config generated
                             from this objects fields, ready to be used to in a RuleBasedProfiler,
                             on success.
                             None, in the case that there exists required fields that are missing.
        """
        if ( #Param Checking
            self.name == None or
            self.version == None or
            self.get_rules() == None
        ):
            return None
        header = self.generate_config_header()
        variables = self.generate_config_variables()
        rules = self.generate_config_rules()
        if rules is None:
            return None
        profiler_config = r""""""
        profiler_config += header
        profiler_config += variables
        profiler_config += rules
        return profiler_config

    def assert_context_initialized(self):
        context = self.context
        if not isinstance(context, ge.DataContext):
            raise TypeError("Expected object of type <great_expectation.DataContext>, instead received: ", + str(type(context)))

    def get_ge_context(self):
        """
        Simple getter that returns this ProfilerConfigGenerator's great_expectations DataContext, if present.

        @Returns:
            On Success, the DataContext stored in this object's 'context' field
            On Failure, None
        """
        return self.context
    
    def set_ge_context(self, context: ge.DataContext):
        """
        Simple setter that sets this objects context field to the one provided.

        Args:
            context: A great_expectations DataContext object, to be stored in this object
        
        Returns:
            old_context: The previous context stored in this object, before replacement, if it exists 
                         Otherwise, None.
        """
        self.assert_context_initialized()
        old_context = self.context
        self.context = context
        return old_context

    def add_ge_context_datasource(self, datasource: Dict[str, Any]):
        """
        Adds the provided datasource config dict to this objects great_expectations context field

        Args:
            datasource: the datasource config dict that will be added to this objects DataContext

        Returns:
            context: On success, returns this objects context field containing a great_expectations DataContext
        
        Exceptions:
            TypeError: Raised when context is not initialized and stores a None type
        """
        self.assert_context_initialized()
        context = self.context
        context.add_datasource(**datasource)

    def generate_and_run_suite_and_checkpoint(self, dataframes: List[pd.DataFrame], title: str, **kwargs):
        """
        Gen and Run suite and checkpoint
        """
        return 0
