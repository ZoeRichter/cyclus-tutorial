def get_resources(agent_ids, flux, cur):
    '''
    This function returns pandas dataframe that contains the agent, time, and
    mass data at each timestep of the simulation.

    Inputs:
    agent_ids: a list of tuples
    flux: str, either 'receiverid', if it is influx, or 'senderid', if outflux.
    cur: sqlite3 cursor pointing to sqlite database of interest

    Outputs:
    resources: pandas dataframe

    '''

    query = ("SELECT" +
             "transactions.senderid as Agent," +
             "transactions.time as Time," +
             "resources.quantity as Quantity," +
             "resources.qualid as Qualid " +
             "FROM resources INNER JOIN transactions " +
             "ON transactions.resourceid = resources.resourceid " +
             "WHERE transactions." + flux + " == " + str(agent_ids[0][0]))
    if len(agent_ids[0]) > 1:
        for agents in agent_ids[1:]:
            query += " or transactions." + flux + " == " + str(agents[0])

    resources_raw = pd.read_sql_query(query, cur)
    resources = resources_raw.drop_duplicates(['Agent','Time']).copy()

    resources['Qualid'] = resources.loc[:,'Qualid'].astype(object)

    for row in resources.index:
        
        agent = resources.loc[row,'Agent']
        time = resources.loc[row,'Time']
        qualdf = resources_raw.query('Agent == @agent & Time == @time')
        qualids = qualdf.loc[:,'Qualid'].values.tolist()
    
        command = 'Agent == @agent & Time == @time & Qualid in @qualids'
        sum_df = resources_raw.query(command)
        summedup = sum_df['Quantity'].sum()
        
        resources.at[row, 'Quantity'] = summedup
        resources.at[row,'Qualid'] = qualids


    agentlist = resources.drop_duplicates('Agent').loc[:, 'Agent'].values

    adict = {}
    for agents in agentlist:
        for col in resources.drop('Agent',axis = 1).columns:
            new_name = col + '_' + str(agents)
            vals = resources.loc[(resources.Agent == agents), col].values
            adict[new_name] = vals

    resources = pd.DataFrame({k:pd.Series(v) for k, v in adict.items()})

    duration = cur.execute("SELECT duration FROM info").fetchone()[0]

    df = pd.DataFrame({'SimTime': np.arange(0, duration + 1, 1)})
    resources = pd.concat([df, resources], axis=1)

    for agents in agentlist:
        name = 'tempQuant_' + str(agents)
        df1 = pd.DataFrame({ name: np.zeros(len(resources.index)) })
        
        name = 'tempQual_' + str(agents)
        df2 = pd.DataFrame({ name: np.zeros(len(resources.index)) })
        
        resources = pd.concat([resources, df1], axis=1)
        resources = pd.concat([resources, df2], axis=1)

    for agents in agentlist:
        Time = 'Time_' + str(agents)
        Quant = 'Quantity_' + str(agents)
        Qual = 'Qualid_' + str(agents)
        tempQuant = 'tempQuant_' + str(agents)
        tempQual = 'tempQual_' + str(agents)
        resources[tempQual] = resources.loc[:,tempQual].astype(object)
        
        for row in resources.index:
                
            times = resources.loc[:, Time].values
    
            if row in times:
                quantity = resources.loc[(resources[Time] == row), Quant]
                qualid = resources.loc[(resources[Time] == row), Qual]
                
                resources.at[row, tempQuant] = quantity.values[0]
                resources.at[row, tempQual] = qualid.values[0]

        resources.at[:,Quant] = resources.loc[:,tempQuant].values
        resources.at[:,Qual] = resources.loc[:,tempQual].values

    select = list(resources.filter(regex='tempQuant'))
    resources.drop(select,axis=1,inplace=True)
                  
    select = list(resources.filter(regex='tempQual'))
    resources.drop(select,axis=1,inplace=True)
                  
    select = list(resources.filter(regex='Time_'))
    resources.drop(select, axis=1, inplace=True)

    return resources


def split_mass_into_iso(resources, cur):

    qual_df = resources.drop_duplicates(
        list(
            resources.filter(
                regex='Qualid_'))).drop(
        list(
            resources.filter(
                regex='Quantity_')),
        axis=1).drop(
        'SimTime',
        axis=1)

    qual_df = qual_df.drop_duplicates()

    quallist = np.zeros(1)
    for columns in qual_df.columns.values:
        quallist = np.concatenate((quallist, qual_df.loc[:, columns].values))

    quallist = quallist[quallist > 0.0]

    compositions = pd.read_sql_query(
        "SELECT qualid as Qualid, nucid, massfrac FROM compositions", cur)

    compositions = compositions[compositions['Qualid'].isin(quallist)]
    nuclist = compositions.drop_duplicates('NucId').loc[:, 'NucId'].values
    agentlist = resources.drop_duplicates(['Agent']).drop(
        columns=['Time', 'Quantity', 'Qualid']).loc[:, 'Agent'].values

    for agent in agentlist:
        for nuc in nuclist:
            df = pd.DataFrame({(str(nuc) + '_' + str(agent)): np.zeros(len(resources.index))})
            resources = pd.concat([resources, df], axis=1)

    for agents in agentlist:
        for row in resources.index.values:
            if resources.loc[row, 'Qualid_' + str(agents)] in quallist:

                qualid = resources.loc[row, 'Qualid_' + str(agents)]
                nucids = compositions.loc[compositions.Qualid ==
                                          qualid, 'NucId'].values

                for nuc in nucids:
                    massfrac = compositions.loc[((compositions.Qualid == qualid) & (
                        compositions.NucId == nuc)), 'MassFrac'].values[0]
                    isomass = (
                        resources.loc[row, 'Quantity_' + str(agents)]) * massfrac
                    resources.at[row, str(nuc) + '_' + str(agents)] = isomass

    return resources, compositions


def plot_in_out_flux(
        cur,
        facility,
        influx_bool,
        title,
        is_cum=False,
        is_tot=False):
    
    agent_ids = cur.execute(
        "SELECT agentid FROM AgentEntry WHERE Prototype = ?", (facility,)).fetchall()

    if influx_bool:
        flux = 'receiverid'
    else:
        flux = 'senderid'

    resources = get_resources(agent_ids, flux, cur)

    if not is_tot:

        resources, compositions = split_mass_into_iso(resources, cur)

        if is_cum:
            for agents in agentlist:
                for row in resources.index:
                    if row > 0:
                        nuclist = compositions.drop_duplicates(
                            'NucId').loc[:, 'NucId'].values
                        for nuc in nuclist:
                            resources.at[row, str(
                                nuc) + '_' + str(agents)] += resourcesloc[(row - 1), str(nuc) + '_' + str(agents)]

    if is_cum & is_tot:

        for agents in agentlist:
            for row in resources.index.values:
                if row > 0:
                    resources.at[row, 'Quantity_' + str(agents)] += resources.loc[(row - 1), 'Quantity_'str(agents)]
