

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.13958104-818d-47ed-84eb-7fbf3a1749e8"),
    Bebtelovimab_data=Input(rid="ri.foundry.main.dataset.c3efe690-072e-4474-a88c-a1ec424b8e31")
)
'''
Selected first treatment date of Bebtelovimab and dropped duplicate entries
'''

import pandas as pd 
def Bebtelovimab(Bebtelovimab_data):
    df = Bebtelovimab_data.toPandas()
    # selected relevant columns for further analysis
    df = df[["person_id","drug_exposure_start_date","drug_exposure_end_date","drug_concept_name"]]
    # sorted dataframe by column condition_start_date
    df.sort_values(by = "drug_exposure_start_date", ascending = True, inplace = True)
    # drop duplicate by person_id
    df.drop_duplicates(subset = 'person_id', keep = 'first', inplace = True)
    df['Bebtelovimab'] = 1
    return df

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.581cc020-4e92-4759-a5b6-407b36d80c3a"),
    Bebtelovimab=Input(rid="ri.foundry.main.dataset.13958104-818d-47ed-84eb-7fbf3a1749e8"),
    molnupiravir=Input(rid="ri.foundry.main.dataset.ee084579-a228-4b18-b4ab-9f0b9757abd8"),
    paxlovid=Input(rid="ri.foundry.main.dataset.57e2f7e7-f671-43fc-ba9d-59a9ab936e36")
)
'''
Merged selected three antivirals together
'''

from pyspark.sql import functions as F 
from functools import reduce
import pandas as pd

def Pax_Lagevrio_Babtelovimav_patients_data(paxlovid, molnupiravir, Bebtelovimab):
    df1 = paxlovid.select('person_id','drug_exposure_start_date','paxlovid').toPandas()
    df2 = molnupiravir.select('person_id','drug_exposure_start_date','molnupiravir').toPandas()
    df3 = Bebtelovimab.select('person_id','drug_exposure_start_date','Bebtelovimab').toPandas()
    data_frames = [df1,df2,df3]    
    df_merge = reduce(lambda left,right: pd.merge(left,right, on =['person_id','drug_exposure_start_date'], how = 'outer'), data_frames).fillna(0)

    return df_merge
    

@transform_pandas(
    Output(rid="ri.vector.main.execute.a9323c46-b007-4569-af6a-0b73971b92bc"),
    Stack_plot_severity_IS_use=Input(rid="ri.foundry.main.dataset.55bbd22f-7f36-43db-9d3b-ae4e063ed2ef")
)
'''
Distribution plot of hospitalization with respect to imunosuppressants.
'''
import matplotlib.pyplot as plt
import matplotlib as mpl
import numpy as np
import pandas as pd
import seaborn as sns

def Severity_hospitalized_Immuno(Stack_plot_severity_IS_use):
    df = Stack_plot_severity_IS_use.toPandas()
    df = df [["person_id","Anthracyclines", "Checkpoint_inhibitor", "Cyclophosphamide", "PK_inhibitor","Monoclonal_other", "Rituximab","Other_antineoplastic_agents", "Azathioprine", "Calcineurin_inhibitor", "IL_inhibitor","JAK_inhibitor","Mycophenol","TNF_inhibitor","Other_selective_immunosuppressants","Glucocorticoid","Severity_Type"]]
    # rename 'Dead_w_COVID' as 'Severe' to group 'Dead_w_COVID' & 'Severe' as a Severe
    df['Severity_Type'] = df['Severity_Type'].replace({'Death_after_COVID_index':'Hospitalized'})
    df['Severity_Type'] = df['Severity_Type'].replace({'Severe_ECMO_IMV_in_Hosp_around_COVID_index':'Hospitalized'})
    df['Severity_Type'] = df['Severity_Type'].replace({'Moderate_Hosp_around_COVID_index':'Hospitalized'})
    df['Severity_Type'] = df['Severity_Type'].replace({'Mild_ED_around_COVID_index':'Not_hospitalized'})
    df['Severity_Type'] = df['Severity_Type'].replace({'Mild_No_ED_or_Hosp_around_COVID_index':'Not_hospitalized'})
    
    df = df [["Anthracyclines", "Checkpoint_inhibitor", "Cyclophosphamide", "PK_inhibitor","Monoclonal_other", "Rituximab","Other_antineoplastic_agents", "Azathioprine", "Calcineurin_inhibitor", "IL_inhibitor","JAK_inhibitor","Mycophenol","TNF_inhibitor","Other_selective_immunosuppressants","Glucocorticoid","Severity_Type"]]
    df1 = df.groupby("Severity_Type").sum()

    index1 = df1.columns.tolist()
    IIndex = [x.replace("_", " ").capitalize() for x in index1]
 
    data = pd.DataFrame({'Immunosupressants': IIndex,'Hospitalized':df1.iloc[0], 'Not hospitalized':df1.iloc[1]})
    data = data.sort_values(by = 'Hospitalized',ascending = False)
    
# Code for plot
    font_color = 'black'#'#525252'
    hfont = {'fontname':'Calibri'}
    facecolor = 'black'#'#eaeaf2'
    color_red = '#ac7e04' #'#fd625e'
    color_blue = '#2242c7'   # '#01b8aa'
    index = data.Immunosupressants
    column0 = data['Hospitalized'] # np.log(
    column1 = data['Not hospitalized']
    title0 = 'Hospitalized'
    title1 = 'Not hospitalized'
    fig, axes = plt.subplots(figsize=(7.5,5), facecolor=facecolor, ncols=2, sharey=True)
    fig.tight_layout()
    
    axes[0].barh(index, column0, align='center', log = True, color=color_red, zorder=10, edgecolor= 'black')
    axes[0].set_title(title0, fontsize=13, pad=15, color=color_red, **hfont)
    axes[1].barh(index, column1, align='center', log = True, color=color_blue, zorder=10, edgecolor= 'black')
    axes[1].set_title(title1, fontsize=13, pad=15, color=color_blue, **hfont)
    
    
    # If you have positive numbers and want to invert the x-axis of the left plot
    axes[0].set_xlim([100,300000])
    axes[0].invert_xaxis() 

    # To show data from highest to lowest
    plt.gca().invert_yaxis()

    axes[0].set(yticks=index, yticklabels=index)
    axes[0].yaxis.tick_left()
    axes[0].tick_params(axis='y', colors='black') # tick color

    axes[1].set_xlim([100,300000])

    for label in (axes[0].get_xticklabels() + axes[0].get_yticklabels()):
        label.set(fontsize=11, color=font_color, **hfont)
    for label in (axes[1].get_xticklabels() + axes[1].get_yticklabels()):
        label.set(fontsize=11, color=font_color, **hfont)
    
    plt.subplots_adjust(wspace=0, top=0.85, bottom=0.1, left=0.5, right=0.95)
    
    plt.show()
 
    
    
    return data

@transform_pandas(
    Output(rid="ri.vector.main.execute.ae33f14e-21b2-4e42-aa07-03c76a90664f"),
    Stack_plot_severity_IS_use=Input(rid="ri.foundry.main.dataset.55bbd22f-7f36-43db-9d3b-ae4e063ed2ef")
)
'''
Distribution plot of life-threatening condition with respect to imunosuppressants.
'''
import matplotlib.pyplot as plt
import matplotlib as mpl
import numpy as np
import pandas as pd
import seaborn as sns

def Severity_life_thretening_immuno(Stack_plot_severity_IS_use):
    df = Stack_plot_severity_IS_use.toPandas()
    df = df [["person_id","Anthracyclines", "Checkpoint_inhibitor", "Cyclophosphamide", "PK_inhibitor","Monoclonal_other", "Rituximab","Other_antineoplastic_agents", "Azathioprine", "Calcineurin_inhibitor", "IL_inhibitor","JAK_inhibitor","Mycophenol","TNF_inhibitor","Other_selective_immunosuppressants","Glucocorticoid","Severity_Type"]]
    # rename 'Dead_w_COVID' as 'Severe' to group 'Dead_w_COVID' & 'Severe' as a Severe
    df['Severity_Type'] = df['Severity_Type'].replace({'Death_after_COVID_index':'Life_thretening'})
    df['Severity_Type'] = df['Severity_Type'].replace({'Severe_ECMO_IMV_in_Hosp_around_COVID_index':'Life_thretening'})
    df['Severity_Type'] = df['Severity_Type'].replace({'Moderate_Hosp_around_COVID_index':'Not_life_thretening'})
    df['Severity_Type'] = df['Severity_Type'].replace({'Mild_ED_around_COVID_index':'Not_life_thretening'})
    df['Severity_Type'] = df['Severity_Type'].replace({'Mild_No_ED_or_Hosp_around_COVID_index':'Not_life_thretening'})
    df = df [["Anthracyclines", "Checkpoint_inhibitor", "Cyclophosphamide", "PK_inhibitor","Monoclonal_other", "Rituximab","Other_antineoplastic_agents", "Azathioprine", "Calcineurin_inhibitor", "IL_inhibitor","JAK_inhibitor","Mycophenol","TNF_inhibitor","Other_selective_immunosuppressants","Glucocorticoid","Severity_Type"]]
    
    df1 = df.groupby("Severity_Type").sum()

    index1 = df1.columns.tolist()
    IIndex = [x.replace("_", " ").capitalize() for x in index1]
 
    data = pd.DataFrame({'Immunosupressants': IIndex,'Life thretening':df1.iloc[0], 'Not life thretening':df1.iloc[1]})
    data = data.sort_values(by = 'Life thretening',ascending = False)
    
# Code for plot
    font_color = 'black'#'#525252'
    hfont = {'fontname':'Calibri'}
    facecolor = 'black'#'#eaeaf2'
    color_red = '#ac7e04' #'#fd625e'
    color_blue = '#2242c7'   # '#01b8aa'
    index = data.Immunosupressants
    column0 = data['Life thretening'] # np.log(
    column1 = data['Not life thretening']
    title0 = 'Life threatening'
    title1 = 'Not life threatening'
    fig, axes = plt.subplots(figsize=(7.5,5), facecolor=facecolor, ncols=2, sharey=True)
    fig.tight_layout()
    
    axes[0].barh(index, column0, align='center', log = True, color=color_red, zorder=10, edgecolor= 'black')
    axes[0].set_title(title0, fontsize=13, pad=15, color=color_red, **hfont)
    axes[1].barh(index, column1, align='center', log = True, color=color_blue, zorder=10, edgecolor= 'black')
    axes[1].set_title(title1, fontsize=13, pad=15, color=color_blue, **hfont)
    
    
    # If you have positive numbers and want to invert the x-axis of the left plot
    axes[0].set_xlim([10,300000])
    axes[0].invert_xaxis() 

    # To show data from highest to lowest
    plt.gca().invert_yaxis()

    axes[0].set(yticks=index, yticklabels=index)
    axes[0].yaxis.tick_left()
    axes[0].tick_params(axis='y', colors='black') # tick color

    axes[1].set_xlim([10,300000])

    for label in (axes[0].get_xticklabels() + axes[0].get_yticklabels()):
        label.set(fontsize=11, color=font_color, **hfont)
    for label in (axes[1].get_xticklabels() + axes[1].get_yticklabels()):
        label.set(fontsize=11, color=font_color, **hfont)
    
  
    plt.subplots_adjust(wspace=0, top=0.85, bottom=0.1, left=0.5, right=0.95)
   
    plt.show()
    
    
    return data

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.ee084579-a228-4b18-b4ab-9f0b9757abd8"),
    molnupiravir_data=Input(rid="ri.foundry.main.dataset.c9225f30-c260-4a9e-86b9-00d517ae02c7")
)
'''
Selected first treatment date of molnupiravir and dropped duplicate entries
'''

import pandas as pd 
def molnupiravir(molnupiravir_data):
    df = molnupiravir_data.toPandas()
    # selected relevant columns for further analysis
    df = df[["person_id","drug_exposure_start_date","drug_exposure_end_date","drug_concept_name"]]
    # sorted dataframe by column condition_start_date
    df.sort_values(by = "drug_exposure_start_date", ascending = True, inplace = True)
    # drop duplicate by person_id
    df.drop_duplicates(subset = 'person_id', keep = 'first', inplace = True)
    df['molnupiravir'] = 1
    return df

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.82472975-bf67-45df-8155-560e994e781c"),
    Patients_immuno_indicator_data=Input(rid="ri.foundry.main.dataset.ad1a2ec9-00c3-415e-b11a-170c80219806")
)
import pandas as pd
import numpy as np
def patients_on_immunosupressants(Patients_immuno_indicator_data):
    df = Patients_immuno_indicator_data.toPandas()
    df = df[['person_id' ,'anthracyclines','checkpoint_inhibitor','cyclophosphamide','pk_inhibitor','monoclonal_other',
'rituximab','l01_other','azathioprine','calcineurin_inhibitor','il_inhibitor','jak_inhibitor','mycophenol',
'tnf_inhibitor','l04_other','glucocorticoid','steroids_before_covid','steroids_during_covid','gluco_dose_known_high']]
    df['glucocorticoid_final'] = df['glucocorticoid'] + df['gluco_dose_known_high']
    df.loc[df['glucocorticoid_final']>0, 'glucocorticoid_final'] = 1
    df.drop(['glucocorticoid','gluco_dose_known_high'], axis = 1 , inplace = True)
    df.rename(columns = {'glucocorticoid_final': 'glucocorticoid'} , inplace = True)
    # rename immunosuppressants 
    df.rename(columns = {'anthracyclines': 'Anthracyclines'} , inplace = True)
    df.rename(columns = {'checkpoint_inhibitor': 'Checkpoint_inhibitor'} , inplace = True)
    df.rename(columns = {'cyclophosphamide': 'Cyclophosphamide'} , inplace = True)
    df.rename(columns = {'pk_inhibitor': 'PK_inhibitor'} , inplace = True)
    df.rename(columns = {'monoclonal_other': 'Monoclonal_other'} , inplace = True)
    df.rename(columns = {'rituximab': 'Rituximab'} , inplace = True)
    df.rename(columns = {'l01_other': 'Other_antineoplastic_agents'} , inplace = True) # Other antineoplastic agents (or L01_other)
    df.rename(columns = {'azathioprine': 'Azathioprine'} , inplace = True)
    df.rename(columns = {'calcineurin_inhibitor': 'Calcineurin_inhibitor'} , inplace = True)
    df.rename(columns = {'il_inhibitor': 'IL_inhibitor'} , inplace = True)
    df.rename(columns = {'jak_inhibitor': 'JAK_inhibitor'} , inplace = True)
    df.rename(columns = {'mycophenol': 'Mycophenol'} , inplace = True)
    df.rename(columns = {'tnf_inhibitor': 'TNF_inhibitor'} , inplace = True)
    df.rename(columns = {'l04_other': 'Other_selective_immunosuppressants'} , inplace = True) # Other selective immunosuppressants (or L04_other)
    df.rename(columns = {'glucocorticoid': 'Glucocorticoid'} , inplace = True)
    return df   
    

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.57e2f7e7-f671-43fc-ba9d-59a9ab936e36"),
    paxlovid_data=Input(rid="ri.foundry.main.dataset.7d90d06b-f217-440c-ae83-dca5b6af99e3")
)
'''
Selected first treatment date of paxlovid and dropped duplicate entries
'''

import pandas as pd 
def paxlovid(paxlovid_data):
    df = paxlovid_data.toPandas()
    # selected relevant columns for further analysis
    df = df[["person_id","drug_exposure_start_date","drug_exposure_end_date","drug_concept_name"]]
    # sorted dataframe by column condition_start_date
    df.sort_values(by = "drug_exposure_start_date", ascending = True, inplace = True)
    # drop duplicate by person_id
    df.drop_duplicates(subset = 'person_id', keep = 'first', inplace = True)
    df['paxlovid'] = 1
    return df

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.f4ce1e03-169c-4d82-a3d1-83e4e548b8f4"),
    Cohort_covid_final=Input(rid="ri.foundry.main.dataset.9fda0be9-ceab-47ae-b159-bec157742c13"),
    Pax_Lagevrio_Babtelovimav_patients_data=Input(rid="ri.foundry.main.dataset.581cc020-4e92-4759-a5b6-407b36d80c3a")
)
'''
Selected antiviral treatment to COVID-19 patients 
'''
import pandas as pd
import numpy as np
from pyspark.sql import functions as F
from pyspark.sql.functions import isnan, when, count, col, lit

def selected_antiviral(Pax_Lagevrio_Babtelovimav_patients_data, Cohort_covid_final):
    
    df1 = Pax_Lagevrio_Babtelovimav_patients_data.select('person_id','drug_exposure_start_date','paxlovid','molnupiravir','Bebtelovimab')
    df2 = Cohort_covid_final.select('person_id','COVID_first_PCR_or_AG_lab_positive')

    df = df1.select('person_id','drug_exposure_start_date','paxlovid','molnupiravir','Bebtelovimab').join(df2,'person_id','inner')
    df = df.withColumn('lab_minus_drug_exposure_start_date', F.datediff('COVID_first_PCR_or_AG_lab_positive','drug_exposure_start_date')).toPandas()

    # Patients considered if antiviral drugs administered after 30 days to index date  
    df = df[(df['lab_minus_drug_exposure_start_date']>= -10) & (df['lab_minus_drug_exposure_start_date']<= 10)] #-30
    # drop duplicate by person_id
    df.drop_duplicates(subset = 'person_id', keep = 'first', inplace = True)
    df['selected_antiviral_treatment'] = 1
    df = df[['person_id','selected_antiviral_treatment','paxlovid','molnupiravir','Bebtelovimab']]
    dff = pd.merge(df2.toPandas(), df, how="outer", on = 'person_id').fillna(0) 
    return dff
 
'''
    # drop duplicate by person_id
    df.drop_duplicates(subset = 'person_id', keep = 'first', inplace = True)
    # patients considered if antiviral drugs administered between 90 days prior to index date and 30 days after index 
    df = df[(df['lab_minus_drug_exposure_start_date']>= -30) & (df['lab_minus_drug_exposure_start_date']<= 30)]
    df['selected_antiviral_treatment'] = 1
    df = df[['person_id','selected_antiviral_treatment']]
    dff = pd.merge(df2.toPandas(), df, how="outer", on = 'person_id').fillna(0) 
    return dff
'''

    

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.cec85faf-1025-46d9-ac41-7f7de38d02c6"),
    patients_on_immunosupressants=Input(rid="ri.foundry.main.dataset.82472975-bf67-45df-8155-560e994e781c")
)
'''
Distribution plot of imunosuppressants
'''

from matplotlib import pyplot as plt
import matplotlib as mpl
import numpy as np
import pandas as pd
import seaborn as sns

def top_immunosuppressants_fig(patients_on_immunosupressants):
    df = patients_on_immunosupressants.toPandas()
    df = df[["Anthracyclines", "Checkpoint_inhibitor", "Cyclophosphamide", "PK_inhibitor","Monoclonal_other", "Rituximab",
"Other_antineoplastic_agents", "Azathioprine", "Calcineurin_inhibitor", "IL_inhibitor","JAK_inhibitor","Mycophenol","TNF_inhibitor",
"Other_selective_immunosuppressants","Glucocorticoid"]]
    columns = df.columns.tolist()
    dd = pd.DataFrame({'Immunosuppressant': columns,'Frequency':df[columns].sum().values})
    df_sorted = dd.sort_values(by = 'Frequency',ascending = False)
    ranking1 = df_sorted.head(20)
    ranking = ranking1.sort_values(by = 'Frequency',ascending = True)

    index1 = ranking['Immunosuppressant'].tolist()
    index = [x.replace("_", " ").capitalize() for x in index1]
    values = ranking['Frequency']
  
    plot_title = 'Immunosuppressant'
    title_size = 18
    subtitle = '' #'Patients with Rare Disease then diagnosis with COVID-19'
    x_label = '# of Patients'
    filename = 'barh-plot'
    
    fig, ax = plt.subplots(figsize=(7.5,4.5), facecolor=(.94, .94, .94)) # 6,6
    mpl.pyplot.viridis()
   
    bar = ax.barh(index, values, log = True,  align = 'center') # True
    plt.tight_layout()
    ax.xaxis.set_major_formatter(mpl.ticker.StrMethodFormatter('{x:,.0f}'))
    plt.subplots_adjust(top=0.9, bottom=0.1)
    def gradientbars(bars):
        grad = np.atleast_2d(np.linspace(0,1,256))
        ax = bars[0].axes
        lim = ax.get_xlim()+ax.get_ylim()
        for bar in bars:
            bar.set_zorder(1)
            bar.set_facecolor('none')
            x,y = bar.get_xy()
            w, h = bar.get_width(), bar.get_height()
            ax.imshow(grad, extent=[x+w, x, y, y+h], aspect='auto', zorder=1)
        ax.axis(lim)
    gradientbars(bar)

    rects = ax.patches
    # Place a label for each bar
    for rect in rects:
        # Get X and Y placement of label from rect
        x_value = rect.get_width()
        y_value = rect.get_y() + rect.get_height() / 2

        # Number of points between bar and label; change to your liking
        space = 2
        # Vertical alignment for positive values
        ha = 'left'

        # If value of bar is negative: place label to the left of the bar
        if x_value < 0:
            # Invert space to place label to the left
            space *= -1
            # Horizontally align label to the right
            ha = 'right'

        # Use X value as label and format number
        label = '{:,.0f}'.format(x_value)

        # Create annotation
        plt.annotate(
            label,                      # Use `label` as label
            (x_value, y_value),         # Place label at bar end
            xytext=(space, 0),          # Horizontally shift label by `space`
            textcoords='offset points', # Interpret `xytext` as offset in points
            va='center',                # Vertically center label
            ha=ha,                      # Horizontally align label differently for positive and negative values
            color = 'blue',fontsize = 11) #13

    #    # Set subtitle
        tfrom = ax.get_xaxis_transform()
        ann = ax.annotate(subtitle, xy=(5, 1), xycoords=tfrom, bbox=dict(boxstyle='square,pad=1.3', fc='#f0f0f0', ec='none'))
        
        #Set x-label
        ax.set_xlabel(x_label, color='k',fontweight='bold')   
        ax.yaxis.set_ticks_position('left')
        ax.xaxis.set_ticks_position('bottom')
        plt.xticks(fontsize = 12) # fontweight='bold'
        plt.yticks(fontsize = 12)
        plt.style.use('classic')
        for pos in ['right', 'top', 'bottom', 'left']:
            plt.gca().spines[pos].set_visible(False)
        plt.xlim(500, 800000)
    plt.tight_layout()
    plt.show()
        
    
    

