import pandas as pd
import numpy as np
import dateutil
import networkx as nx

ADULT_AGE = 18

def get_hmis_cp():
    """
    Pull in relevant CSVs from `../data/`, merge them, clean them, and return a tuple containing the cleaned HMIS data
    and the cleaned Connecting Point data.
    """
    # get raw dataframes
    hmis = get_raw_hmis()
    cp = get_raw_cp()

    # convert dates
    hmis = hmis_convert_dates(hmis)
    cp = cp_convert_dates(cp)

    # compute client and family ids across the dataframes
    (hmis, cp) = get_client_family_ids(hmis, cp)

    # get child status
    hmis = hmis_child_status(hmis)
    cp = cp_child_status(cp)

    # generate family characteristics
    hmis_generate_family_characteristics(hmis)
    cp_generate_family_characteristics(cp)

    return (hmis, cp)

###################
# get_raw methods #
###################

def get_raw_hmis():
    """
    Pull in relevant CSVs from `../data/`, merge them, and return the raw HMIS dataframe.
    """
    program = pd.read_csv('../data/hmis/program with family.csv')
    client = pd.read_csv('../data/hmis/client de-identified.csv')
    # NOTE we're taking an inner join here because the program csv got pulled after
    # the client csv, because we added the family site identifier column to program
    program = program.merge(client, on='Subject Unique Identifier', how='inner')

    return program

def get_raw_cp():
    """
    Pull in relevant CSVs from `../data/`, merge them, and return the raw Connecting Point dataframe.
    """
    case = pd.read_csv("../data/connecting_point/case.csv")
    case = case.rename(columns={'caseid': 'Caseid'})
    client = pd.read_csv("../data/connecting_point/client.csv")
    case = case.merge(client, on='Caseid', how='left')

    return case

#############################################
# get_client_family_ids and related methods #
#############################################

def get_client_family_ids(hmis, cp):
    """
    Given raw HMIS and Connecting Point dataframes, de-duplicate individuals and determine families across time.

    See the README for more information about rationale and methodology.

    The graph contains IDs from both HMIS and Connecting Point, so each vertex is represented as a tuple `(c, id)`,
    where `c` is either `'h'` or `'c'`, to indicate whether the `id` corresponds to a row in HMIS or Connecting Point.
    For example, `('h', 1234)` represents the row(s) in HMIS with individual ID `1234`, and `('c',5678)` represents the
    row(s) in Connecting Point with individual ID `5678`.

    :param hmis: HMIS dataframe.
    :type hmis: Pandas.Dataframe.

    :param cp: Connecting Point dataframe.
    :type cp: Pandas.Dataframe.
    """
    hmis = hmis.rename(columns={'Subject Unique Identifier': 'Raw Subject Unique Identifier'})
    cp = cp.rename(columns={'Clientid': 'Raw Clientid'})

    # create graph of individuals
    G_individuals = nx.Graph()
    G_individuals.add_nodes_from([('h', v) for v in hmis['Raw Subject Unique Identifier'].values])
    G_individuals.add_nodes_from([('c', v) for v in cp['Raw Clientid'].values])

    # add edges between same individuals
    G_individuals.add_edges_from(group_edges('h', pd.read_csv('../data/hmis/hmis_client_duplicates_link_plus.csv'), ['Set ID'], 'Subject Unique Identifier'))
    G_individuals.add_edges_from(group_edges('c', pd.read_csv('../data/connecting_point/cp_client_duplicates_link_plus.csv'), ['Set ID'], 'Clientid'))
    G_individuals.add_edges_from(matching_edges())

    # copy graph of individuals and add edges between individuals in the same family
    G_families = G_individuals.copy()
    G_families.add_edges_from(group_edges('h', hmis, ['Family Site Identifier','Program Start Date'], 'Raw Subject Unique Identifier'))
    G_families.add_edges_from(group_edges('c', cp, ['Caseid'], 'Raw Clientid'))

    # compute connected components and pull out ids for each dataframe for individuals and families
    hmis_individuals = [get_ids_from_nodes('h', c) for c in nx.connected_components(G_individuals)]
    cp_individuals = [get_ids_from_nodes('c', c) for c in nx.connected_components(G_individuals)]
    hmis_families = [get_ids_from_nodes('h', c) for c in nx.connected_components(G_families)]
    cp_families = [get_ids_from_nodes('c', c) for c in nx.connected_components(G_families)]

    # create dataframes to merge
    hmis_individuals = create_dataframe_from_grouped_ids(hmis_individuals, 'Subject Unique Identifier')
    hmis_families = create_dataframe_from_grouped_ids(hmis_families, 'Family Identifier')
    cp_individuals = create_dataframe_from_grouped_ids(cp_individuals, 'Clientid')
    cp_families = create_dataframe_from_grouped_ids(cp_families, 'Familyid')

    # merge into hmis and cp dataframes
    hmis = hmis.merge(hmis_individuals, left_on='Raw Subject Unique Identifier', right_index=True, how='left')
    hmis = hmis.merge(hmis_families, left_on='Raw Subject Unique Identifier', right_index=True, how='left')
    cp = cp.merge(cp_individuals, left_on='Raw Clientid', right_index=True, how='left')
    cp = cp.merge(cp_families, left_on='Raw Clientid', right_index=True, how='left')

    return (hmis, cp)

def group_edges(node_prefix, df, group_ids, individual_id):
    """
    Return the edge list from a grouping dataframe, either a Link Plus fuzzy matching or a dataframe, where people are
    connected by appearing in the same family or case.

    :param node_prefix: prefix for the nodes in the edge list.
    :type node_prefix: str.

    :param df: dataframe.
    :type df: Pandas.Dataframe.

    :param group_ids: grouping column names in grouping csv.
    :type group_ids: [str].

    :param individual_id: individual id column name in grouping csv.
    :type individual_id: str.
    """
    groups = df[group_ids+[individual_id]].dropna().drop_duplicates().set_index(group_ids)
    edges = groups.merge(groups, left_index=True, right_index=True)
    return [tuple(map(lambda v: (node_prefix, v), e)) for e in edges.values]

def matching_edges():
    """
    Returns the edge list from a Connecting Point to HMIS matching CSV.
    """
    matching = pd.read_csv('../data/matching/cp_hmis_match_results.csv').dropna()
    return [(('c',v[0]),('h',v[1])) for v in matching[['clientid','Subject Unique Identifier']].values]

def get_ids_from_nodes(node_prefix, nodes):
    """
    Take a list of nodes from G and returns a list of the ids of only the nodes with the given prefix.

    param node_prefix: prefix for the nodes to keep.
    type node_prefix: str.

    param nodes: list of nodes from G.
    type nodes: [(str, int)].
    """
    return map(lambda pair: pair[1], filter(lambda pair: pair[0] == node_prefix, nodes))

def create_dataframe_from_grouped_ids(grouped_ids, col):
    """
    Take a list of IDs, grouped by individual or family, and creates a dataframe where each ID in a group has the same
    id in `col`.

    For example, for the arguments `[[1, 2, 3], [4, 5, 6], [7, 8], [9]]` and `'Family Identifier'`, return a single-column dataframe:

    ```
      Family Identifier
    -+-----------------
    1 0
    2 0
    3 0
    4 1
    5 1
    6 1
    7 2
    8 2
    9 3
    ```

    param grouped_ids: a list of lists of ids.
    type grouped_ids: [[int]].

    param col: the name to give the single column in the dataframe.
    type col: str.
    """
    return pd.DataFrame({col: pd.Series({id: idx for idx, ids in enumerate(grouped_ids) for id in ids})})

#########################
# convert_dates methods #
#########################

def hmis_convert_dates(hmis):
    """
    Given an HMIS dataframe, convert columns with dates to datetime columns.

    :param hmis: HMIS dataframe.
    :type hmis: Pandas.Dataframe.
    """
    hmis['Raw Program Start Date'] = hmis['Program Start Date']
    hmis['Program Start Date'] = pd.to_datetime(hmis['Program Start Date'])
    hmis['Raw Program End Date'] = hmis['Program End Date']
    hmis['Program End Date'] = pd.to_datetime(hmis['Program End Date'])
    hmis['Raw DOB'] = hmis['DOB']
    hmis['DOB'] = pd.to_datetime(hmis['DOB'])

    return hmis

def cp_convert_dates(cp):
    """
    Given a Connecting Point dataframe, convert columns with dates to datetime columns.

    :param cp: Connecting Point dataframe.
    :type cp: Pandas.Dataframe.
    """
    cp['Raw servstart'] = cp['servstart']
    cp['servstart'] = pd.to_datetime(cp['servstart'])
    cp['Raw servend'] = cp['servend']
    cp['servend'] = pd.to_datetime(cp['servend'])
    cp['Raw LastUpdateDate'] = cp['LastUpdateDate']
    cp['LastUpdateDate'] = pd.to_datetime(cp['LastUpdateDate'])

    return cp

####################################
# child_status and related methods #
####################################

def hmis_child_status(hmis):
    """
    Given an HMIS dataframe, add `Child?` and `Adult?` columns.

    :param hmis: HMIS dataframe.
    :type hmis: Pandas.Dataframe.
    """
    hmis['Age Entered'] = hmis.apply(get_hmis_age_entered, axis=1)
    hmis['Child?'] = hmis['Age Entered'] < ADULT_AGE
    hmis['Adult?'] = ~hmis['Child?']

    return hmis

def get_hmis_age_entered(row):
    """
    Given an HMIS row, compute the age of the client.

    :param row: HMIS row.
    :type row: Pandas.Series.
    """
    start_date = row['Program Start Date']
    dob = row['DOB']
    if start_date is pd.NaT or dob is pd.NaT:
        return np.NaN
    else:
        return dateutil.relativedelta.relativedelta(start_date, dob).years

def cp_child_status(cp):
    """
    Given a Connecting Point dataframe, add `Child?` and `Adult?` columns.

    :param cp: Connecting Point dataframe.
    :type cp: Pandas.Dataframe.
    """
    cp['Child?'] = cp['age'] < ADULT_AGE
    cp['Adult?'] = ~cp['Child?']

    return cp

##############################################
# family_characteristics and related methods #
##############################################

def hmis_generate_family_characteristics(hmis):
    """
    Given an HMIS dataframe, add columns regarding family structure.

    :param hmis: HMIS dataframe.
    :type hmis: Pandas.Dataframe.
    """
    return generate_family_characteristics(hmis, family_id='Family Identifier', group_ids=['Family Site Identifier', 'Program Start Date'])

def cp_generate_family_characteristics(cp):
    """
    Given a Connecting Point dataframe, add columns regarding family structure.

    :param cp: Connecting Point dataframe.
    :type cp: Pandas.Dataframe.
    """
    return generate_family_characteristics(cp, family_id='Familyid', group_ids=['Caseid'])

def generate_family_characteristics(df, family_id, group_ids):
    """
    Given either an HMIS or a Connecting Point dataframe, add columns regarding family structure.

    :param df: HMIS or Connecting point dataframe.
    :type hmis: Pandas.Dataframe.

    :param family_id: column name of family identifier.
    :type family_id: str.

    :param group_ids: grouping column names.
    :type group_ids: [str].
    """
    df['With Child?'] = df.groupby(group_ids)['Child?'].transform(any)
    df['With Adult?'] = df.groupby(group_ids)['Adult?'].transform(any)
    df['With Family?'] = df['With Child?'] & df['With Adult?']
    df['Family?'] = df.groupby(family_id)['With Family?'].transform(any)
    return df
