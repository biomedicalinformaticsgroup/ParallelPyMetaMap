# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

""" extract_concepts takes a list of sentences and ids(optional)
    then returns a list of Concept objects extracted via
    MetaMap.
    Supported Options:
    Composite Phrase -Q
    Word Sense Disambiguation -y
    use strict model -A
    use relaxed model -C
    allow large N -l
    allow overmatches -o
    allow concept gaps -g
    term processing -z
    No Derivational Variants -d
    All Derivational Variants -D
    Ignore Word Order -i
    Allow Acronym Variants -a
    Unique Acronym Variants -u
    Prefer Multiple Concepts -Y
    Ignore Stop Phrases -K
    Compute All Mappings -b
    MM Data Version -V
    Exclude Sources -e
    Restrict to Sources -R
    Restrict to Semantic Types -J
    Exclude Semantic Types -k
    Suppress Numerical Concepts --no_nums
    For information about the available options visit
    http://metamap.nlm.nih.gov/.
    Note: If an error is encountered the process will be closed
    and whatever was processed, if anything, will be
    returned along with the error found.
    """

import os
import subprocess
import tempfile
from .MetaMap import MetaMap
from .Concept import Corpus

class SubprocessBackend(MetaMap):
    def __init__(self, metamap_filename, version=None):
        """ Interface to MetaMap using subprocess. This creates a
            command line call to a specified metamap process.
        """
        MetaMap.__init__(self, metamap_filename, version)

    def extract_concepts(self,
                         sentences=None,
                         ids=None,
                         composite_phrase=4,
                         fielded_mmi_output=False,
                         machine_output=False,
                         filename=None,
                         file_format='sldi',
                         allow_acronym_variants=False,
                         word_sense_disambiguation=False,
                         allow_large_n=False,
                         strict_model=False,
                         relaxed_model=False,
                         allow_overmatches=False,
                         allow_concept_gaps=False,
                         term_processing=False,
                         no_derivational_variants=False,
                         derivational_variants=False,
                         ignore_word_order=False,
                         unique_acronym_variants=False,
                         prefer_multiple_concepts=False,
                         ignore_stop_phrases=False,
                         compute_all_mappings=False,
                         prune=False,
                         mm_data_version=False,
                         mm_data_year=False,
                         verbose=False,
                         exclude_sources=[],
                         restrict_to_sources=[],
                         restrict_to_sts=[],
                         exclude_sts=[],
                         no_nums=[]):

        if allow_acronym_variants and unique_acronym_variants:
            raise ValueError("You can't use both allow_acronym_variants and "
                             "unique_acronym_variants.")
        if (sentences is not None and filename is not None) or \
                (sentences is None and filename is None):
            raise ValueError("You must either pass a list of sentences "
                             "OR a filename.")
        if file_format not in ['sldi','sldiID']:
            raise ValueError("file_format must be either sldi or sldiID")
        if (fielded_mmi_output is False and machine_output is False) or \
                (fielded_mmi_output is True and machine_output is True):
            raise ValueError("You must choose between fielded_mmi_output "
                             "OR machine_output.")



        input_file = None


        if sentences is not None:
            input_file = tempfile.NamedTemporaryFile(mode="wb", delete=False)
        else:
            input_file = open(filename, 'r')
        output_file = tempfile.NamedTemporaryFile(mode="r", delete=False)
        error = None
        
        try:
            if sentences is not None:
                if ids is not None:
                    for identifier, sentence in zip(ids, sentences):
                        input_file.write('{0!r}|{1!r}\n'.format(identifier, sentence).encode('utf8'))
                else:
                    for sentence in sentences:
                        input_file.write('{0!r}\n'.format(sentence).encode('utf8'))
                input_file.flush()
            if fielded_mmi_output:
                command = [self.metamap_filename, '-N']
            if machine_output:
                command = [self.metamap_filename, '-q']
            command.append('-Q')
            command.append(str(composite_phrase))
            if mm_data_version is not False:
                if mm_data_version not in ['Base', 'USAbase', 'NLM']:
                    raise ValueError("mm_data_version must be Base, USAbase, or NLM.")
                command.append('-V')
                command.append(str(mm_data_version))
            if mm_data_year is not False:
                command.append('-Z')
                command.append(str(mm_data_year))
            if word_sense_disambiguation:
                command.append('-y')
            if strict_model:
                command.append('-A')
            if prune is not False:
                command.append('--prune')
                command.append(str(prune))
            if relaxed_model:
                command.append('-C')
            if allow_large_n:
                command.append('-l')
            if allow_overmatches:
                command.append('-o')
            if allow_concept_gaps:
                command.append('-g')
            if term_processing:
                command.append('-z')
            if no_derivational_variants:
                command.append('-d')
            if derivational_variants:
                command.append('-D')
            if ignore_word_order:
                command.append('-i')
            if allow_acronym_variants:
                command.append('-a')
            if unique_acronym_variants:
                command.append('-u')
            if prefer_multiple_concepts:
                command.append('-Y')
            if ignore_stop_phrases:
                command.append('-K')
            if compute_all_mappings:
                command.append('-b')
            if len(exclude_sources) > 0:
                command.append('-e')
                command.append(str(','.join(exclude_sources)))
            if len(restrict_to_sources) > 0:
                command.append('-R')
                command.append(str(','.join(restrict_to_sources)))
            if len(restrict_to_sts) > 0:
                command.append('-J')
                command.append(str(','.join(restrict_to_sts)))
            if len(exclude_sts) > 0:
                command.append('-k')
                command.append(str(','.join(exclude_sts)))
            if len(no_nums) > 0:
                command.append('--no_nums')
                command.append(str(','.join(no_nums)))
            if ids is not None or (file_format == 'sldiID' and sentences is None):
                command.append('--sldiID')
            else:
                command.append('--sldi')
            if verbose == False:
                command.append('--silent')
            else:
                pass


            command.append(input_file.name)
            command.append(output_file.name)

            metamap_process = subprocess.Popen(command, stdout=subprocess.PIPE)
            while metamap_process.poll() is None:
                stdout = str(metamap_process.stdout.readline())
                if ('ERROR' or 'WARNING') in stdout:
                    metamap_process.terminate()
                    error = stdout.rstrip()
            output = str(output_file.read())
        
        finally:
            if sentences is not None:
                os.remove(input_file.name)
            else:
                input_file.close()
            os.remove(output_file.name)
        if fielded_mmi_output:
            concepts = Corpus.load(output.splitlines())
        if machine_output:
            concepts = output.splitlines()
        return (concepts, error)
