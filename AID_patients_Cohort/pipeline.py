

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.b26fe40c-9a59-4697-bc8b-b65ed02061d9"),
    AID_priot_Covid_data=Input(rid="ri.foundry.main.dataset.4270dec0-748b-496a-a03c-01aa972053f4")
)
import pyspark.sql.functions as F
import pandas as pd   
# binarize the AID variables and droped three features
def AID_before_covid_final(AID_priot_Covid_data):
    df = AID_priot_Covid_data.toPandas()
    df1 = df[['person_id', 'data_partner_id', 'COVID_first_PCR_or_AG_lab_positive']]
    df.drop(['person_id', 'data_partner_id', 'COVID_first_PCR_or_AG_lab_positive'], axis = 1, inplace = True)
    df[-df.isnull()] = 1 
    df.fillna(0,inplace = True)
    dff = pd.concat([df1,df],axis =1)
    return dff
    
    

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.cf581ec5-e0ab-4dc5-8a8a-5df619d960f0"),
    AID_data_sorted_SNCTID=Input(rid="ri.foundry.main.dataset.20f35db1-0d5e-4eaa-b8ca-cb7720c5ad8a")
)
import pandas as pd
import numpy as np
# grouped AID (Rheumatoid, Multiple Sclerosis, LUPUS, Autoimmune thyroiditis)
def AID_name_grouped(AID_data_sorted_SNCTID):
    df = AID_data_sorted_SNCTID.toPandas()
    df = df[['person_id','data_partner_id','COVID_first_PCR_or_AG_lab_positive','condition_concept_name','condition_start_date']]
    # here we grouped autoimmune disease 
    df['condition_concept_name'] = df['condition_concept_name'].replace({
        'Seropositive rheumatoid arthritis': 'Rheumatoid arthritis',
        'Rheumatoid arthritis of multiple joints': 'Rheumatoid arthritis',
        'Rheumatoid arthritis - hand joint': 'Rheumatoid arthritis',
        'Rheumatoid arthritis of knee':'Rheumatoid arthritis',
        'Rheumatoid factor positive rheumatoid arthritis':'Rheumatoid arthritis',
        'Rheumatoid arthritis of right hand': 'Rheumatoid arthritis',
        'Bilateral rheumatoid arthritis of hands': 'Rheumatoid arthritis',
        'Flare of rheumatoid arthritis': 'Rheumatoid arthritis',
        'Rheumatoid arthritis of right foot': 'Rheumatoid arthritis',
        'Rheumatoid arthritis of bilateral hips': 'Rheumatoid arthritis',
        'Bilateral rheumatoid arthritis of knees': 'Rheumatoid arthritis',
        'Rheumatoid arthritis of left ankle': 'Rheumatoid arthritis',
        'Relapsing remitting multiple sclerosis': 'Multiple sclerosis',
        'Exacerbation of multiple sclerosis': 'Multiple sclerosis',
        'Primary progressive multiple sclerosis': 'Multiple sclerosis',
        'Secondary progressive multiple sclerosis': 'Multiple sclerosis', 
        'Systemic lupus erythematosus': 'Lupus',
        'SLE glomerulonephritis syndrome': 'Lupus',
        'Systemic lupus erythematosus with organ/system involvement' : 'Lupus',
        'Lupus erythematosus': 'Lupus',
        'Systemic lupus erythematosus-related syndrome': 'Lupus',
        'Cutaneous lupus erythematosus': 'Lupus',
        'Acute systemic lupus erythematosus': 'Lupus',
        'Lupus erythematosus overlap syndrome': 'Lupus',
        'Neonatal lupus erythematosus': 'Lupus',
        'SLE glomerulonephritis syndrome, WHO class V' : 'Lupus',
        'Discoid lupus erythematosus' : 'Lupus',
        'Fibrous autoimmune thyroiditis': 'Autoimmune thyroiditis', 
        'Steroid-responsive encephalopathy associated with autoimmune thyroiditis': 'Autoimmune thyroiditis',
        'Chronic idiopathic thrombocytopenic purpura': 'Idiopathic thrombocytopenic purpura',
        'Acute idiopathic thrombocytopenic purpura':'Idiopathic thrombocytopenic purpura',
        'Immune thrombocytopenia':'Idiopathic thrombocytopenic purpura',
        'CREST syndrome': 'Systemic sclerosis',
        'Lung disease with systemic sclerosis': 'Systemic sclerosis',
        'Progressive systemic sclerosis': 'Systemic sclerosis',
        'Limited systemic sclerosis': 'Systemic sclerosis',
        'Sclerodema': 'Systemic sclerosis',
        'Systemic sclerosis with limited cutaneous involvement' : 'Systemic sclerosis', 
        'Chronic autoimmune hepatitis': 'Autoimmune hepatitis',
        'Cold autoimmune hemolytic anemia': 'Autoimmune hemolytic anemia',
        'Drug-induced autoimmune hemolytic anemia': 'Autoimmune hemolytic anemia',
        "Meniere's disease of left inner ear" :"Meniere's disease", 
        "Ménière's disease": "Meniere's disease",
        "Sjögren's syndrome" : "Sjogren's syndrome",
        'Microscopic polyarteritis nodosa': 'Polyarteritis nodosa',
        'Cutaneous polyarteritis nodosa' :'Polyarteritis nodosa',
        "Crohn's disease" : 'Inflammatory bowel disease',
        'Ulcerative colitis':'Inflammatory bowel disease', 
        'Myasthenia gravis with exacerbation': 'Myasthenia gravis' })
    return df

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.4270dec0-748b-496a-a03c-01aa972053f4"),
    AID_name_grouped=Input(rid="ri.foundry.main.dataset.cf581ec5-e0ab-4dc5-8a8a-5df619d960f0")
)
#--- selected unique AID patients with its first reported AIDs 
from pyspark.sql import functions as F
def AID_priot_Covid_data(AID_name_grouped):
    df = AID_name_grouped
    df = df.withColumn("condition_concept_name", F.regexp_replace(df["condition_concept_name"], "\s+", "_" ))
    df = df.withColumn("condition_concept_name", F.regexp_replace(F.lower(df["condition_concept_name"]), "[^A-Za-z_0-9]", "" ))
    p = df.groupby("person_id", "data_partner_id",'COVID_first_PCR_or_AG_lab_positive').pivot("condition_concept_name").agg(F.min('condition_start_date'))
        
    p.repartition(10)

    return p

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.86c1065a-5e27-4c74-94ce-fbf0cc07a4c7"),
    AID_before_covid_final=Input(rid="ri.foundry.main.dataset.b26fe40c-9a59-4697-bc8b-b65ed02061d9"),
    Type1_dm_prior_covid=Input(rid="ri.foundry.main.dataset.1cf04aaf-b81e-4ba7-bd9d-d9b7083a342c"),
    psoriasis_prior_covid=Input(rid="ri.foundry.main.dataset.f2827e14-7f2d-477c-97d7-5e3486dc3460")
)
'''
Following code block used to merge all AID with Type1DM and Psoriasis to create complete AID cohort
'''
import pandas as pd
import numpy as np

def AIDs_cohort(AID_before_covid_final, Type1_dm_prior_covid, psoriasis_prior_covid):
    df1 = AID_before_covid_final.toPandas()
    df2 = Type1_dm_prior_covid.toPandas()
    df3 = psoriasis_prior_covid.toPandas()
    df2 =df2[['person_id','Type_1_diabetes_mellitus']]
    df3 = df3 [['person_id','psoriasis']]
    df = pd.merge(pd.merge(df1,df2, on = 'person_id',how = 'outer'), df3 , on = 'person_id',how = 'outer')
    df.drop(['data_partner_id','COVID_first_PCR_or_AG_lab_positive'], axis = 1, inplace = True)
    return df.fillna(0)
    

@transform_pandas(
    Output(rid="ri.vector.main.execute.74fce5e1-0067-4d9f-bcb0-ac1cd3b79305"),
    AID_with_severity=Input(rid="ri.foundry.main.dataset.8f848245-5aff-401c-81a5-b7aa980faa92")
)
'''
Distribution plot of hospitalization severity with respect to top 20 autoimmune diseases.
'''

import matplotlib.pyplot as plt
import matplotlib as mpl
import numpy as np
import pandas as pd
import seaborn as sns

print('numpy version is:',np.__version__)
print('pandas version is:',pd.__version__)
print('matplotlib version is:', mpl.__version__)
print('Seaborn version is:', sns.__version__)

def Severity_plot_hospitalized_AID(AID_with_severity):
    df = AID_with_severity.toPandas()

    # rename AID long name 
    df.rename(columns = {'megaloblastic_anemia_due_to_vitamin_b12_deficiency': 'megaloblastic_anemia_(B12_deficiency)'} , inplace = True)

    # rename 'Dead_w_COVID' as 'Severe' to group 'Dead_w_COVID' & 'Severe' as a Severe
    df['Severity_Type'] = df['Severity_Type'].replace({'Death_after_COVID_index':'Hospitalized'})
    df['Severity_Type'] = df['Severity_Type'].replace({'Severe_ECMO_IMV_in_Hosp_around_COVID_index':'Hospitalized'})
    df['Severity_Type'] = df['Severity_Type'].replace({'Moderate_Hosp_around_COVID_index':'Hospitalized'})
    df['Severity_Type'] = df['Severity_Type'].replace({'Mild_ED_around_COVID_index':'Not_hospitalized'})
    df['Severity_Type'] = df['Severity_Type'].replace({'Mild_No_ED_or_Hosp_around_COVID_index':'Not_hospitalized'})
    column_to_drop = ['person_id','Severity_Type']    
    # drop non numeric features 
    main_list = df.columns.tolist()
    list_final = [elm for elm in main_list if elm not in column_to_drop] 
    # dataframe with numeric features
    df1 = df[list_final]
    columns = df1.columns.tolist()
    # sorted top 20 AIDs from total AIDs
    dd = pd.DataFrame({'AID': columns,'Frequency':df1[columns].sum().values})
    df_sorted = dd.sort_values(by = 'Frequency',ascending = False)
    ranking = df_sorted.head(20)
    # dataframe with top 20 AIDs and severity typr
    col = set(ranking.AID.tolist()).union(['Severity_Type'])
    df1 = df[list(col)]
    # grouped top 20 AIDs by severity type and sorted decending order
    df2 = df1.groupby('Severity_Type')[ranking.AID.tolist()].sum()

    plot_data = pd.DataFrame(df2.T, index = df2.columns.tolist(), columns = df2.index)
    plot_data = plot_data.sort_values(by = 'Hospitalized',ascending = False)
    
    index1 = plot_data.index.tolist()
    IIndex = [x.replace("_", " ").capitalize() for x in index1]
    print(IIndex)
    print(df.shape[0])

    plot_data = plot_data.rename(index = dict(zip(index1 ,IIndex )))

# Code for plot
    font_color = 'black'#'#525252'
    hfont = {'fontname':'Calibri'}
    facecolor = 'black'#'#eaeaf2'
    color_red = '#ac7e04' #'#fd625e'
    color_blue = '#2242c7'   # '#01b8aa'
    index = IIndex
    column0 = plot_data['Hospitalized'] # np.log(
    column1 = plot_data['Not_hospitalized']
    title0 = 'Hospitalized'
    title1 = 'Not hospitalized'
    fig, axes = plt.subplots(figsize=(7.5,7.5), facecolor=facecolor , ncols=2, sharey=True)
    fig.tight_layout()
    
    axes[0].barh(index, column0, align='center', log = True, color=color_red, zorder=10, edgecolor= 'black')
    axes[0].set_title(title0, fontsize=13, pad=15, color=color_red, **hfont)
    axes[1].barh(index, column1, align='center', log = True, color=color_blue, zorder=10, edgecolor= 'black')
    axes[1].set_title(title1, fontsize=13, pad=15, color=color_blue, **hfont)
    
    # If you have positive numbers and want to invert the x-axis of the left plot
    axes[0].set_xlim([10,30000])
    axes[0].invert_xaxis() 

    # To show data from highest to lowest
    plt.gca().invert_yaxis()

    axes[0].set(yticks=index, yticklabels=index)
    axes[0].yaxis.tick_left()
    axes[0].tick_params(axis='y', colors='black') # tick color

    axes[1].set_xlim([10,30000])

    for label in (axes[0].get_xticklabels() + axes[0].get_yticklabels()):
        label.set(fontsize=11, color=font_color, **hfont)
    for label in (axes[1].get_xticklabels() + axes[1].get_yticklabels()):
        label.set(fontsize=11, color=font_color, **hfont)
    
    plt.subplots_adjust(wspace=0, top=0.85, bottom=0.1, left=0.50, right=0.95)
    
    plt.show()

    return plot_data

@transform_pandas(
    Output(rid="ri.vector.main.execute.b0857492-50a3-46d6-9ff0-e82d5e7a8b2f"),
    AID_with_severity=Input(rid="ri.foundry.main.dataset.8f848245-5aff-401c-81a5-b7aa980faa92")
)
'''
Distribution plot of life-threatening condition with respect to top 20 autoimmune diseases.
'''

import matplotlib.pyplot as plt
import matplotlib as mpl
import numpy as np
import pandas as pd
import seaborn as sns

print('numpy version is:',np.__version__)
print('pandas version is:',pd.__version__)
print('matplotlib version is:', mpl.__version__)
print('Seaborn version is:', sns.__version__)

def Severity_plot_life_thretening_AID(AID_with_severity):
    df = AID_with_severity.toPandas()
    
    # rename AID long name  (megaloblastic_anemia_due_to_vitamin_b12_deficiency)
    df.rename(columns = {'megaloblastic_anemia_due_to_vitamin_b12_deficiency': 'megaloblastic_anemia_(B12_deficiency)'} , inplace = True)

    # rename 'Dead_w_COVID' as 'Severe' to group 'Dead_w_COVID' & 'Severe' as a Severe
    df['Severity_Type'] = df['Severity_Type'].replace({'Death_after_COVID_index':'Life_thretening'})
    df['Severity_Type'] = df['Severity_Type'].replace({'Severe_ECMO_IMV_in_Hosp_around_COVID_index':'Life_thretening'})
    df['Severity_Type'] = df['Severity_Type'].replace({'Moderate_Hosp_around_COVID_index':'Not_life_thretening'})
    df['Severity_Type'] = df['Severity_Type'].replace({'Mild_ED_around_COVID_index':'Not_life_thretening'})
    df['Severity_Type'] = df['Severity_Type'].replace({'Mild_No_ED_or_Hosp_around_COVID_index':'Not_life_thretening'})
    column_to_drop = ['person_id','Severity_Type']
    # drop non numeric features 
    main_list = df.columns.tolist()
    list_final = [elm for elm in main_list if elm not in column_to_drop] 
    # dataframe with numeric features
    df1 = df[list_final]
    columns = df1.columns.tolist()
    # sorted top 20 AIDs from total AIDs
    dd = pd.DataFrame({'AID': columns,'Frequency':df1[columns].sum().values})
    df_sorted = dd.sort_values(by = 'Frequency',ascending = False)
    ranking = df_sorted.head(20)
    # dataframe with top 20 AIDs and severity typr
    col = set(ranking.AID.tolist()).union(['Severity_Type'])
    df1 = df[list(col)]
    # grouped top 20 AIDs by severity type and sorted decending order
    df2 = df1.groupby('Severity_Type')[ranking.AID.tolist()].sum()

    plot_data = pd.DataFrame(df2.T, index = df2.columns.tolist(), columns = df2.index)
    plot_data = plot_data.sort_values(by = 'Life_thretening',ascending = False)

    
    index1 = plot_data.index.tolist()
    IIndex = [x.replace("_", " ").capitalize() for x in index1]
    print(IIndex)
    print(df.shape[0])

    plot_data = plot_data.rename(index = dict(zip(index1 ,IIndex )))

# Code for plot
    font_color = 'black'#'#525252'
    hfont = {'fontname':'Calibri'}
    facecolor = 'black'#'#eaeaf2'
    color_red = '#ac7e04' #'#fd625e'
    color_blue = '#2242c7'   # '#01b8aa'
    index = IIndex
    column0 = plot_data['Life_thretening'] # np.log(
    column1 = plot_data['Not_life_thretening']
    title0 = 'Life threatening'
    title1 = 'Not life threatening'
    fig, axes = plt.subplots(figsize=(7.5,7.5), ncols=2, sharey=True)
    fig.tight_layout()
    
    axes[0].barh(index, column0, align='center', log = True, color=color_red, zorder=10) # , edgecolor= 'black'
    axes[0].set_title(title0, fontsize=13, pad=15, color=color_red, **hfont)
    axes[1].barh(index, column1, align='center', log = True, color=color_blue, zorder=10) # , edgecolor= 'black'
    axes[1].set_title(title1, fontsize=13, pad=15, color=color_blue, **hfont)
    
    
    # If you have positive numbers and want to invert the x-axis of the left plot
    axes[0].set_xlim([10,30000])
    axes[0].invert_xaxis() 

    # To show data from highest to lowest
    plt.gca().invert_yaxis()

    axes[0].set(yticks=index, yticklabels=index)
    axes[0].yaxis.tick_left()
    axes[0].tick_params(axis='y', colors='black') # tick color

    axes[1].set_xlim([10,30000])

    for label in (axes[0].get_xticklabels() + axes[0].get_yticklabels()):
        label.set(fontsize=11, color=font_color, **hfont)
    for label in (axes[1].get_xticklabels() + axes[1].get_yticklabels()):
        label.set(fontsize=11, color=font_color, **hfont)
    
    plt.subplots_adjust(wspace=0, top=0.85, bottom=0.1, left=0.50, right=0.95)
    
    plt.show()

    return plot_data

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.991a45a4-89ce-4e99-9c4b-2afabf7f4639"),
    Psoriasis_data=Input(rid="ri.foundry.main.dataset.c7dadda5-dfed-4db7-92a5-ea4bda53ce81")
)
'''
Following codes used to select first Type1 DM diagnosis date and dropped the 
duplicate entris of same patients if their Type 1 DM recorded multiple times
'''

import pandas as pd

def psoriasis_AID(Psoriasis_data):
    Psoriasis_data_test = Psoriasis_data
    df = Psoriasis_data_test.toPandas()
    # selected relevant columns for further analysis
    df = df[["person_id","condition_start_date","condition_end_date","condition_concept_name"]]
    # sorted dataframe by column condition_start_date
    df.sort_values(by = "condition_start_date", ascending = True, inplace = True)
    # drop duplicate by person_id
    df.drop_duplicates(subset = 'person_id', keep = 'first', inplace = True)
    df['psoriasis'] = 1
    
    return df
    

@transform_pandas(
    Output(rid="ri.vector.main.execute.bd854213-fc38-445c-b21f-c76ca34d2082"),
    AIDs_cohort=Input(rid="ri.foundry.main.dataset.86c1065a-5e27-4c74-94ce-fbf0cc07a4c7")
)
'''
FOllowing code of top 20 AID distribution plot
'''
import matplotlib.pyplot as plt
import matplotlib as mpl
import numpy as np
import pandas as pd
import seaborn as sns

print('numpy version is:',np.__version__)
print('pandas version is:',pd.__version__)
print('matplotlib version is:', mpl.__version__)
print('seaborn version is:', sns.__version__)

def top_20_AID_fig(AIDs_cohort):
    df = AIDs_cohort.toPandas()
    # rename AID long name  (megaloblastic_anemia_due_to_vitamin_b12_deficiency)
    df.rename(columns = {'megaloblastic_anemia_due_to_vitamin_b12_deficiency': 'megaloblastic_anemia_(B12_deficiency)'} , inplace = True)
    
    df.drop(['person_id'], axis = 1, inplace = True)
    columns = df.columns.tolist()
    dd = pd.DataFrame({'AID': columns,'Frequency':df[columns].sum().values})
    df_sorted = dd.sort_values(by = 'Frequency',ascending = False)
    ranking = df_sorted.head(20)
    ranking1 = ranking.sort_values(by = 'Frequency',ascending = True)
        
    index1 = ranking1['AID'].tolist()
    index = [x.replace("_", " ").capitalize() for x in index1]
    values = ranking1['Frequency']
    
    plot_title = 'Top 20 Autoimmune Disease'
    title_size = 18
    subtitle = '' 
    x_label = '# of Patients'
    filename = 'barh-plot'
    
    fig, ax = plt.subplots(figsize=(7.5,6), facecolor=(.94, .94, .94)) # 
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
            color = 'blue',fontsize = 11) 

        # Set subtitle
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

    plt.xlim(1000, 500000)
    plt.tight_layout()
    plt.show() 
    return ranking1

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.aefb7cd1-92ca-461c-9a94-51e8737b5f34"),
    Type1_DM_data=Input(rid="ri.foundry.main.dataset.eb208948-4eca-4cde-b47d-b55fcbdaf909")
)
'''
Following codes used to select first Type1 DM diagnosis date and dropped the 
duplicate entris of same patients if their Type 1 DM recorded multiple times
'''
import pandas as pd 
def type1_DM_AID(Type1_DM_data):
    df = Type1_DM_data.toPandas()
    # selected relevant columns for further analysis
    df = df[["person_id","condition_start_date","condition_end_date","condition_concept_name"]]
    # sorted dataframe by column condition_start_date
    df.sort_values(by = "condition_start_date", ascending = True, inplace = True)
    # drop duplicate by person_id
    df.drop_duplicates(subset = 'person_id', keep = 'first', inplace = True)
    df['Type_1_diabetes_mellitus'] = 1
    return df

