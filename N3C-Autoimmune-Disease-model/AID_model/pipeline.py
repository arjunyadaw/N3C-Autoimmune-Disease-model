

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.12728a5e-97f6-42ea-b7b3-1a7ad9793c6b"),
    Aids_cohort=Input(rid="ri.foundry.main.dataset.86c1065a-5e27-4c74-94ce-fbf0cc07a4c7")
)
from pyspark.sql.functions import isnan, when, count, col, lit
from pyspark.sql import functions as F

def AID_binary(Aids_cohort):
    df = Aids_cohort
    #    Create a new variables AID with 1
    df1 = df.withColumn('AID', lit(1))
    return df1
    

@transform_pandas(
    Output(rid="ri.vector.main.execute.1a6b56d8-3e98-4ad2-b138-b59839372e65"),
    Aids_cohort=Input(rid="ri.foundry.main.dataset.86c1065a-5e27-4c74-94ce-fbf0cc07a4c7"),
    Patients_on_immunosupressants=Input(rid="ri.foundry.main.dataset.82472975-bf67-45df-8155-560e994e781c")
)
'''
Plot figure for usage of immunosuppressants one or more than one.
Similar plot for patients with one pre-existing and more than one AID. 
'''
import matplotlib.pyplot as plt
import matplotlib as mpl
import numpy as np
import pandas as pd
import seaborn as sns

def AID_immuno_frequency_plot(Patients_on_immunosupressants, Aids_cohort):
    df = Aids_cohort.toPandas()
    df1 = Patients_on_immunosupressants.toPandas()
    df1 = df1[["Anthracyclines", "Checkpoint_inhibitor", "Cyclophosphamide", "PK_inhibitor","Monoclonal_other", "Rituximab",
"Other_antineoplastic_agents", "Azathioprine", "Calcineurin_inhibitor", "IL_inhibitor","JAK_inhibitor","Mycophenol","TNF_inhibitor",
"Other_selective_immunosuppressants","Glucocorticoid"]]
    df1['immuno_use'] = df1.sum(axis = 1) 
    
    index2  = df1['immuno_use'].value_counts().index.tolist()
    values2 = df1['immuno_use'].value_counts().values.tolist()

    df.drop(['person_id'], axis = 1, inplace = True)
    df['AID_sum'] = df.sum(axis = 1)
    
    index1  = df['AID_sum'].value_counts().index.tolist()
    values1 = df['AID_sum'].value_counts().values.tolist()

    index = ['# Autoimmune disease = 1', '# Autoimmune disease > 1', '# Immunosuppressants = 1', '# Immunosuppressants > 1']
    values = [values1[0], np.sum(values1[1:]), values2[0], np.sum(values2[1:])  ]
    
    plot_title = ''
    title_size = 20
    subtitle = '' 
    x_label = '# of Patients'
    filename = 'barh-plot'
    
    fig, ax = plt.subplots(figsize=(7,2))
    mpl.pyplot.viridis()
   
    bar = ax.barh(index, values,   align = 'center', linewidth=1.5)# , log = True, , edgecolor= 'black'
    plt.tight_layout()
    ax.xaxis.set_major_formatter(mpl.ticker.StrMethodFormatter('{x:,.0f}'))
    
    #title = plt.title(plot_title, pad=20, fontsize=title_size)
    #title.set_position([.43, 1])
    plt.subplots_adjust(top=0.9, bottom=0.1)

    #ax.grid(zorder=0)

    def gradientbars(bars):
        grad = np.atleast_2d(np.linspace(0,1,256))
        ax = bars[0].axes
        lim = ax.get_xlim()+ax.get_ylim()
        for bar in bars:
            bar.set_zorder(1)
            bar.set_facecolor('none')
        #    bar.set_edgecolor('black')
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
            color = 'blue', fontsize = 11) 

        # Set subtitle
        tfrom = ax.get_xaxis_transform()
        ann = ax.annotate(subtitle, xy=(5, 1), xycoords=tfrom, bbox=dict(boxstyle='square, pad=1.3', fc='#f0f0f0', ec='none'))

        #Set x-label 
       
        ax.set_xlabel(x_label, color='k',fontweight='bold',fontsize = 10)   
        ax.yaxis.set_ticks_position('left')
        ax.xaxis.set_ticks_position('bottom')
        plt.xticks(fontsize = 12) # fontweight='bold'
        plt.yticks(fontsize = 12)
        plt.style.use('classic')
        for pos in ['right', 'top', 'bottom', 'left']:
            plt.gca().spines[pos].set_visible(False)
        plt.xlim((1000, 250000))  
    plt.tight_layout()

    plt.show()

    

@transform_pandas(
    Output(rid="ri.vector.main.execute.1385fd7b-9be4-4bd1-b69b-2e332fa5f2ac"),
    hospitalized=Input(rid="ri.foundry.main.dataset.69ed467b-2aa0-4e55-8c57-c43fc037ee53")
)
'''
Following code for univariate, multivariate logistic regression models of COVID-19
severity (life-treatening) association with prior immunosuppressants treatments (yes/no) of AID patients 
'''
import sys
import pandas as pd
import numpy as np
from patsy import dmatrices
import statsmodels.api as sm
import patsy
print('numpy version is:',np.__version__)
print('pandas version is:',pd.__version__)
print('statsmodel version is:',sm.__version__)
print('patsy version is:', patsy.__version__)
from pyspark.sql import functions as F
from pyspark.sql.functions import col, when

def ALL_Immuno_multi_life_hospitalized_AID_model(hospitalized):
    df = hospitalized
    df = df.select("covid_diagnosis_date","Age","BMI","COVID_vaccine_doses","Long_covid","Severity","AID","Anthracyclines", "Checkpoint_inhibitor", "Cyclophosphamide", "PK_inhibitor","Monoclonal_other", "Rituximab","L01_other", "Azathioprine", "Calcineurin_inhibitor", "IL_inhibitor","JAK_inhibitor","Mycophenol","TNF_inhibitor","L04_other","Glucocorticoid","MI","CHF","PVD","stroke","dementia","pulmonary","liver_mild","liversevere","type2_diabetes","renal","cancer","mets","hiv","Gender_FEMALE","Gender_MALE","Race_Asian","Race_Black_or_African_American","Race_Native_Hawaiian_or_Other_Pacific_Islander","Race_Unknown","Race_White","Ethnicity_Hispanic_or_Latino","Ethnicity_Not_Hispanic_or_Latino","Ethnicity_Unknown","smoking_status_Current_or_Former","smoking_status_Non_smoker",'selected_antiviral_treatment').toPandas()

# select AID patients data (with/without other immunosuppressants)
    df = df[df["AID"]==1]

    print('yes:',df['Severity'].sum())
    print('No:',df.shape[0]-df['Severity'].sum())
    print('tnf inhibitor:',df['TNF_inhibitor'].sum())

    df['cardiovascular_disease'] = df['MI'] + df['CHF'] + df['PVD'] + df['stroke']
    df.loc[df['cardiovascular_disease']>0, 'cardiovascular_disease'] = 1

### multivariate model demographic and comorbidities and other immunosuppressants
    y, X = dmatrices( 'Severity ~ Anthracyclines + Checkpoint_inhibitor + Cyclophosphamide + PK_inhibitor + Monoclonal_other + Rituximab + L01_other + Azathioprine + Calcineurin_inhibitor + IL_inhibitor + JAK_inhibitor + Mycophenol + TNF_inhibitor + L04_other + Glucocorticoid + Age + BMI + Gender_MALE + Race_Asian + Race_Black_or_African_American + Race_Unknown + Ethnicity_Hispanic_or_Latino + smoking_status_Current_or_Former + cardiovascular_disease +  dementia + pulmonary + liver_mild + liversevere + renal + cancer + mets +  type2_diabetes + hiv', data=df, return_type='dataframe') 

###############

    mod = sm.Logit(y, X)
    res = mod.fit()
    print (res.summary()) 
    
    names = pd.Series(mod.data.xnames)
    names = names.str.replace("_", " ")
    originals = list(names.copy())
    coefficients = res.params.values
    # Created DataFrame of Coefficients, Odds ratio, confidence intervals, p-valus and features
    coefs = pd.DataFrame({
        'coef': ['%0.3f' % elem for elem in coefficients], 
        'odds ratio': ['%0.3f' % elem for elem in np.exp(res.params.values)], 
        'Lower CI' :  ['%0.3f' % elem for elem in np.exp(res.conf_int()[0].values)], 
        'Upper CI' :  ['%0.3f' % elem for elem in np.exp(res.conf_int()[1].values)], 
        'pvalue': res.pvalues,
        'original': originals
    }).sort_values(by='odds ratio', ascending=False)
    return coefs

 
    

@transform_pandas(
    Output(rid="ri.vector.main.execute.ee4206f7-1020-49bc-be96-66cc46cabc53"),
    life_thretening=Input(rid="ri.foundry.main.dataset.b200dee4-fec5-4323-88c8-e096f63f3e9f")
)
'''
Following code for univariate, multivariate logistic regression models of COVID-19
severity (life-treatening) association with prior immunosuppressants treatments (yes/no) of AID patients 
'''
import sys
import pandas as pd
import numpy as np
from patsy import dmatrices
import statsmodels.api as sm
import patsy
print('numpy version is:',np.__version__)
print('pandas version is:',pd.__version__)
print('statsmodel version is:',sm.__version__)
print('patsy version is:', patsy.__version__)
from pyspark.sql import functions as F
from pyspark.sql.functions import col, when

def ALL_immuno_multi_life_thretening_AID_model(life_thretening):
    df = life_thretening
    df = df.select("covid_diagnosis_date","Age","BMI","COVID_vaccine_doses","Long_covid","Severity","AID","Anthracyclines", "Checkpoint_inhibitor", "Cyclophosphamide", "PK_inhibitor","Monoclonal_other", "Rituximab","L01_other", "Azathioprine", "Calcineurin_inhibitor", "IL_inhibitor","JAK_inhibitor","Mycophenol","TNF_inhibitor","L04_other","Glucocorticoid","MI","CHF","PVD","stroke","dementia","pulmonary","liver_mild","liversevere","type2_diabetes","renal","cancer","mets","hiv","Gender_FEMALE","Gender_MALE","Race_Asian","Race_Black_or_African_American","Race_Native_Hawaiian_or_Other_Pacific_Islander","Race_Unknown","Race_White","Ethnicity_Hispanic_or_Latino","Ethnicity_Not_Hispanic_or_Latino","Ethnicity_Unknown","smoking_status_Current_or_Former","smoking_status_Non_smoker",'selected_antiviral_treatment').toPandas()

# select AID patients data (with/without TNF inhibitor and other immunosuppressants)
    df = df[df["AID"]==1]

    print('yes:',df['Severity'].sum())
    print('No:',df.shape[0]-df['Severity'].sum())
    print('tnf inhibitor:',df['TNF_inhibitor'].sum())

    df['cardiovascular_disease'] = df['MI'] + df['CHF'] + df['PVD'] + df['stroke']
    df.loc[df['cardiovascular_disease']>0, 'cardiovascular_disease'] = 1

### multivariate model demographic and comorbidities  and other immunosuppressants
    y, X = dmatrices( 'Severity ~ Anthracyclines + Checkpoint_inhibitor + Cyclophosphamide + PK_inhibitor + Monoclonal_other + Rituximab + L01_other + Azathioprine + Calcineurin_inhibitor + IL_inhibitor + JAK_inhibitor + Mycophenol + TNF_inhibitor + L04_other + Glucocorticoid + Age + BMI + Gender_MALE + Race_Asian + Race_Black_or_African_American + Race_Unknown + Ethnicity_Hispanic_or_Latino + smoking_status_Current_or_Former + cardiovascular_disease +  dementia + pulmonary + liver_mild + liversevere + renal + cancer + mets +  type2_diabetes + hiv', data=df, return_type='dataframe') 

###############

    mod = sm.Logit(y, X)
    res = mod.fit()
    print (res.summary()) 
    
    names = pd.Series(mod.data.xnames)
    names = names.str.replace("_", " ")
    originals = list(names.copy())
    coefficients = res.params.values
    # Created DataFrame of Coefficients, Odds ratio, confidence intervals, p-valus and features
    coefs = pd.DataFrame({
        'coef': ['%0.3f' % elem for elem in coefficients], 
        'odds ratio': ['%0.3f' % elem for elem in np.exp(res.params.values)], 
        'Lower CI' :  ['%0.3f' % elem for elem in np.exp(res.conf_int()[0].values)], 
        'Upper CI' :  ['%0.3f' % elem for elem in np.exp(res.conf_int()[1].values)], 
        'pvalue': res.pvalues,
        'original': originals
    }).sort_values(by='odds ratio', ascending=False)
    return coefs

 
    

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.1e543a28-45e8-47cf-8660-a06bc674602c"),
    cohort_AID_immunosuppressants=Input(rid="ri.foundry.main.dataset.3659b631-b19a-4358-b4d7-8d75bebdcbc3")
)
# Following codes are to create characteristic table by using TableOne library
# Import numerical libraries (PyArrow)
import csv
import pandas as pd
import numpy as np
from scipy import stats
import matplotlib.pyplot as plt
from pyspark.sql import functions as F
from pyspark.sql.functions import col, when
from tableone import TableOne
from openpyxl import Workbook

# Enable Arrow-based columnar data transfers
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")

def Characteristic_table_AID(cohort_AID_immunosuppressants):
    df = cohort_AID_immunosuppressants
    df = df.select("Age","BMI","Gender","Race","Ethnicity","smoking_status","Severity_Type","AID","Long_covid","immunosuppressants","MI","CHF","PVD","stroke","paralysis","dementia","pulmonary","liver_mild","liversevere","type2_diabetes","renal","cancer","mets","hiv") # 
    # Creating life-threatening column based on severity type (Death & Severe)
    df = df.withColumn("life_thretening",
                     when(col("Severity_Type")=='Death', 1)
                     .when(col("Severity_Type")=='Severe', 1)
                     .otherwise(0))
    # Creating hospitalized column based on severity type (Death, Severe & Moderate)
    df = df.withColumn("hospitalized",
                     when(col("Severity_Type")=='Death', 1)
                     .when(col("Severity_Type")=='Severe', 1)
                     .when(col("Severity_Type")=='Moderate', 1)
                     .otherwise(0)).toPandas()
       
    # Creating cardiovascular_disease column based on four comorbidities(MI, CHF, PVD, Stoke)
    df['cardiovascular_disease'] = df['MI'] + df['CHF'] + df['PVD'] + df['stroke'] 
    df.loc[df['cardiovascular_disease']>0, 'cardiovascular_disease'] = 1

    columns = ["Age","BMI","Gender","Race","Ethnicity","smoking_status","life_thretening","hospitalized", "cardiovascular_disease","AID","Long_covid" ,"immunosuppressants","MI","CHF","PVD","stroke", "paralysis","dementia","pulmonary","liver_mild","liversevere","type2_diabetes","renal","cancer","mets","hiv"]    # "paralysis",

    categorical = ["Gender","Race","Ethnicity","smoking_status","life_thretening","hospitalized", "cardiovascular_disease","AID","Long_covid","immunosuppressants","MI","CHF","PVD","stroke","paralysis","dementia","pulmonary","liver_mild","liversevere","type2_diabetes","renal","cancer","mets","hiv"] # ,"paralysis"
    
    # Groupby AID to create characteristic table with/without AID. Similarly groupby immunosuppressants

    ## Create characteristic table for autoimmune disease (Yes/No) 
    groupby = ["AID"]
    
    ## Create characteristic table for Immunosuppressants (Yes/No)
#    groupby = ["immunosuppressants"]

    #nonnormal = ["Age","BMI"]
    mytable = TableOne(df, columns=columns, categorical=categorical, groupby=groupby, pval= True)    # rename=labels,

    print(mytable.tabulate(tablefmt = "fancy_grid"))
    mytable.to_excel('mytable.xlsx')
    mytable.to_csv("Table1.csv")
    return df

    

@transform_pandas(
    Output(rid="ri.vector.main.execute.0e5c592a-e8ad-49ba-b148-0811329b6aae"),
    life_thretening=Input(rid="ri.foundry.main.dataset.b200dee4-fec5-4323-88c8-e096f63f3e9f")
)
'''
Following code for univariate, multivariate logistic regression models of COVID-19
severity(life-treatening) association with pre-existing AID, prior exposure of IS or both (AID+IS). 
stratified by gender
'''
import sys
import pandas as pd
import numpy as np
from patsy import dmatrices
import statsmodels.api as sm
import patsy
print('numpy version is:',np.__version__)
print('pandas version is:',pd.__version__)
print('statsmodel version is:',sm.__version__)
print('patsy version is:', patsy.__version__)
from pyspark.sql import functions as F
from pyspark.sql.functions import *
from pyspark.sql.functions import col, when, sum

def Gender_Male_AID_IS_model(life_thretening):
    df = life_thretening

# Model for all variants including Omicron (Comment both conditions above) 
    df = df.select("covid_diagnosis_date","Age","BMI","COVID_vaccine_doses","Long_covid","Severity","AID","immunosuppressants","TNF_inhibitor","MI","CHF","PVD","stroke","dementia","pulmonary","liver_mild","liversevere","type2_diabetes","renal","cancer","mets","hiv","Gender_FEMALE","Gender_MALE","Race_Asian","Race_Black_or_African_American","Race_Native_Hawaiian_or_Other_Pacific_Islander","Race_Unknown","Race_White","Ethnicity_Hispanic_or_Latino","Ethnicity_Not_Hispanic_or_Latino","Ethnicity_Unknown","smoking_status_Current_or_Former","smoking_status_Non_smoker",'selected_antiviral_treatment')#.toPandas() 

    df_new = df.withColumn('new_immunosuppressants',2*F.col('immunosuppressants')).drop('immunosuppressants')
    df = df_new.withColumn('AID_IS', df_new['new_immunosuppressants'] + df_new['AID'])

    # renamed Severity_types long named into small names
    df = df.withColumn("AID_IS",regexp_replace("AID_IS",'1','AID'))
    df = df.withColumn("AID_IS",regexp_replace("AID_IS",'2','IS'))
    df = df.withColumn("AID_IS",regexp_replace("AID_IS",'3','AID_immuno'))
    df = df.withColumn("AID_IS",regexp_replace("AID_IS",'0','control'))

    df = df.where(F.col('covid_diagnosis_date') <= '2022-06-30').toPandas() 

    df['cardiovascular_disease'] = df['MI'] + df['CHF'] + df['PVD'] + df['stroke'] 
    df.loc[df['cardiovascular_disease']>0, 'cardiovascular_disease'] = 1
    
############ Model for Gender ##########
## stratified for Gender_MALE
#    df = df[df["Gender_MALE"] == 1]

# stratified for Gender_FEMALE
    df = df[df["Gender_FEMALE"] == 1]

    print('Gender_yes:',df['Severity'].sum())
    print('Gender_No:',df.shape[0]-df['Severity'].sum())

    print('yes:',df['Severity'].sum())
    print('No:',df.shape[0]-df['Severity'].sum())
    print('lifethretening_yes:',df['Severity'].sum())
    print('lifethretening_No:',df.shape[0]-df['Severity'].sum())

    ##### univariate model
#    l = ['control','AID','IS','AID_immuno']
#   y, X = dmatrices( 'Severity ~ C(AID_IS, levels = l)', data=df, return_type='dataframe')        
    #####################

    ### multivariate model demographic only ###
#    l = ['control','AID','IS','AID_immuno']
#    y, X = dmatrices( 'Severity ~ C(AID_IS, levels = l) + Age + BMI + Race_Asian + Race_Black_or_African_American + Race_Unknown + Ethnicity_Hispanic_or_Latino + smoking_status_Current_or_Former', data=df, return_type='dataframe')
    ###########################################   

    ### multivariate model demographic and comorbidities ###  C(AID_IS, levels = l):cardiovascular_disease
    l = ['control','AID','IS','AID_immuno']
    y, X = dmatrices( 'Severity ~ C(AID_IS, levels = l) + Age + BMI + Race_Asian + Race_Black_or_African_American + Race_Unknown + Ethnicity_Hispanic_or_Latino + smoking_status_Current_or_Former + cardiovascular_disease + dementia + pulmonary + liver_mild + liversevere + renal + cancer + mets + type2_diabetes + hiv', data=df, return_type='dataframe')

    ###########################################
    print(X.sum())
    mod = sm.Logit(y, X)
    res = mod.fit()
    print (res.summary()) 
    
    names = pd.Series(mod.data.xnames)
    names = names.str.replace("_", " ")
    originals = list(names.copy())
    coefficients = res.params.values
    # Created DataFrame of Coefficients, Odds ratio, confidence intervals, p-valus and features
    coefs = pd.DataFrame({
        'coef': ['%0.3f' % elem for elem in coefficients], 
        'odds ratio': ['%0.3f' % elem for elem in np.exp(res.params.values)], 
        'Lower CI' :  ['%0.3f' % elem for elem in np.exp(res.conf_int()[0].values)], 
        'Upper CI' :  ['%0.3f' % elem for elem in np.exp(res.conf_int()[1].values)], 
        'pvalue': res.pvalues,
        'original': originals
    })#.sort_values(by='odds ratio', ascending=False)
    return coefs

@transform_pandas(
    Output(rid="ri.vector.main.execute.b789c979-fd59-47f1-82b0-25b774de5c81"),
    hospitalized=Input(rid="ri.foundry.main.dataset.69ed467b-2aa0-4e55-8c57-c43fc037ee53")
)
'''
Following code for univariate, multivariate logistic regression models of COVID-19
severity(hospitalization) association with pre-existing AID, prior exposure of IS or both (AID+IS). 
'''
import sys
import pandas as pd
import numpy as np
from patsy import dmatrices
import statsmodels.api as sm
import patsy
print('numpy version is:',np.__version__)
print('pandas version is:',pd.__version__)
print('statsmodel version is:',sm.__version__)
print('patsy version is:', patsy.__version__)
from pyspark.sql import functions as F
from pyspark.sql.functions import *
from pyspark.sql.functions import col, when, sum

def Hospitalized_uni_multi_AID_immunosuppressants(hospitalized):
    df = hospitalized

    df = df.select("covid_diagnosis_date","Age","BMI","COVID_vaccine_doses","Long_covid","Severity","AID","immunosuppressants","TNF_inhibitor","MI","CHF","PVD","stroke","dementia","pulmonary","liver_mild","liversevere","type2_diabetes","renal","cancer","mets","hiv","Gender_FEMALE","Gender_MALE","Race_Asian","Race_Black_or_African_American","Race_Native_Hawaiian_or_Other_Pacific_Islander","Race_Unknown","Race_White","Ethnicity_Hispanic_or_Latino","Ethnicity_Not_Hispanic_or_Latino","Ethnicity_Unknown","smoking_status_Current_or_Former","smoking_status_Non_smoker",'selected_antiviral_treatment')

    df_new = df.withColumn('new_immunosuppressants',2*F.col('immunosuppressants')).drop('immunosuppressants')
    df = df_new.withColumn('AID_IS', df_new['new_immunosuppressants'] + df_new['AID'])

    # renamed Severity_types long named into small names
    df = df.withColumn("AID_IS",regexp_replace("AID_IS",'1','AID'))
    df = df.withColumn("AID_IS",regexp_replace("AID_IS",'2','IS'))
    df = df.withColumn("AID_IS",regexp_replace("AID_IS",'3','AID_immuno'))
    df = df.withColumn("AID_IS",regexp_replace("AID_IS",'0','control'))

    df = df.where(F.col('covid_diagnosis_date') <= '2022-06-30').toPandas() # or '2022-06-30','2021-12-18'
    
    print('Hospitalized_yes:',df['Severity'].sum())
    print('Hospitalized_no:',df.shape[0]-df['Severity'].sum())

    df['cardiovascular_disease'] = df['MI'] + df['CHF'] + df['PVD'] + df['stroke'] 
    df.loc[df['cardiovascular_disease']>0, 'cardiovascular_disease'] = 1

    ##### univariate model
#    l = ['control','AID','IS','AID_immuno']
#    y, X = dmatrices( 'Severity ~ C(AID_IS, levels = l)', data=df, return_type='dataframe')        
    #####################

    ### multivariate model demographic only ###
    l = ['control','AID','IS','AID_immuno']
    y, X = dmatrices( 'Severity ~ C(AID_IS, levels = l) + Age + BMI + Gender_MALE + Race_Asian + Race_Black_or_African_American + Race_Unknown + Ethnicity_Hispanic_or_Latino + smoking_status_Current_or_Former', data=df, return_type='dataframe')
    ###########################################   

    ### multivariate model demographic and comorbidities ###  C(AID_IS, levels = l):cardiovascular_disease
#    l = ['control','AID','IS','AID_immuno']
#    y, X = dmatrices( 'Severity ~ C(AID_IS, levels = l) + Age + BMI + Gender_MALE + Race_Asian + Race_Black_or_African_American + Race_Unknown + Ethnicity_Hispanic_or_Latino + smoking_status_Current_or_Former + cardiovascular_disease + dementia + pulmonary + liver_mild + liversevere + renal + cancer + mets + type2_diabetes + hiv', data=df, return_type='dataframe')
    #print(X.sum())
    ###########################################

    mod = sm.Logit(y, X)
    res = mod.fit()
    print (res.summary()) 
    
    names = pd.Series(mod.data.xnames)
    names = names.str.replace("_", " ")
    originals = list(names.copy())
    coefficients = res.params.values
    # Created DataFrame of Coefficients, Odds ratio, confidence intervals, p-valus and features
    coefs = pd.DataFrame({
        'coef': ['%0.3f' % elem for elem in coefficients], 
        'odds ratio': ['%0.3f' % elem for elem in np.exp(res.params.values)], 
        'Lower CI' :  ['%0.3f' % elem for elem in np.exp(res.conf_int()[0].values)], 
        'Upper CI' :  ['%0.3f' % elem for elem in np.exp(res.conf_int()[1].values)], 
        'pvalue': res.pvalues,
        'original': originals
    })#.sort_values(by='odds ratio', ascending=False)
    return coefs

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.3659b631-b19a-4358-b4d7-8d75bebdcbc3"),
    AID_binary=Input(rid="ri.foundry.main.dataset.12728a5e-97f6-42ea-b7b3-1a7ad9793c6b"),
    Selected_antiviral=Input(rid="ri.foundry.main.dataset.f4ce1e03-169c-4d82-a3d1-83e4e548b8f4"),
    covid_cohort_sept=Input(rid="ri.foundry.main.dataset.fd795399-d564-4534-ac43-fc6d6e8362e3"),
    immunosuppressants=Input(rid="ri.foundry.main.dataset.4ece3b88-6daf-47df-bd49-5b09c28060e9")
)

from pyspark.sql import functions as F
def cohort_AID_immunosuppressants(covid_cohort_sept, AID_binary, immunosuppressants, Selected_antiviral):
    df1 = covid_cohort_sept
    df2 = immunosuppressants
    df3 = AID_binary
    df = Selected_antiviral.select('person_id','selected_antiviral_treatment', 'Bebtelovimab','paxlovid','molnupiravir')
    # Join dataframes (AID, immunosuppressant and antivirals) with COVID-19 cohort
    df4 = df1.join(df2, on=['person_id'], how='left')
    df5 = df4.join(df3, on=['person_id'], how='left')
    df6 = df5.join(df,  on=['person_id'], how='left')
    # Replace NaN with 0 except BMI.  
    df_final = df6.na.fill(value=0, subset = [col for col in df6.columns if col not in ('BMI')])
    return df_final 

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.29865cb1-f9a7-4a09-a556-ca0a6f592983"),
    cohort_AID_immunosuppressants=Input(rid="ri.foundry.main.dataset.3659b631-b19a-4358-b4d7-8d75bebdcbc3")
)
"""
Final cohort with demographic, comorbidities, long COVID, Vaccination status, 
usage of immunosuppressants, pre-existing AID and selected antiviral treatments  etc.
"""
from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit, regexp_replace
import pandas as pd
import numpy as np

def cohort_final(cohort_AID_immunosuppressants):
    df = cohort_AID_immunosuppressants
    # select columns for further analysis
    df = df.select("person_id","data_partner_id","COVID_first_PCR_or_AG_lab_positive","Age","BMI","number_of_COVID_vaccine_doses_before_covid","Long_covid","Length_of_stay","Severity_Type","AID","Anthracyclines", "Checkpoint_inhibitor", "Cyclophosphamide", "PK_inhibitor","Monoclonal_other", "Rituximab","Other_antineoplastic_agents", "Azathioprine", "Calcineurin_inhibitor", "IL_inhibitor","JAK_inhibitor","Mycophenol","TNF_inhibitor","Other_selective_immunosuppressants","Glucocorticoid","steroids_before_covid","steroids_during_covid","immunosuppressants","Gender","Ethnicity","Race","smoking_status","MI","CHF","PVD","stroke","paralysis","dementia","pulmonary","liver_mild","liversevere","type2_diabetes","renal","cancer","mets","hiv","COVID_associated_hospitalization_indicator",'selected_antiviral_treatment', 'Bebtelovimab','paxlovid','molnupiravir')
   

    # renamed columns with long names 
    df = df.withColumnRenamed('COVID_first_PCR_or_AG_lab_positive','covid_diagnosis_date')\
           .withColumnRenamed('number_of_COVID_vaccine_doses_before_covid','COVID_vaccine_doses').toPandas()
    # impute BMI with its mean value
    df['BMI'].fillna(value=df['BMI'].mean(), inplace=True)
    # Ecodes object type columns in binary variables 
    encode_cols = ['Gender','Race', 'Ethnicity','smoking_status']
    encoded_data = pd.get_dummies(df, columns = encode_cols, prefix = encode_cols, drop_first = False)
    
    return encoded_data

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.fd795399-d564-4534-ac43-fc6d6e8362e3"),
    Cohort_covid_positive=Input(rid="ri.foundry.main.dataset.d0dbdf74-0a3f-4d93-a7e3-d175b6e1ab2c")
)
from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit, regexp_replace
import numpy as np

def covid_cohort_sept(Cohort_covid_positive):
    df = Cohort_covid_positive
    # select columns for further analysis
    df = df.select("person_id","data_partner_id","COVID_first_PCR_or_AG_lab_positive","number_of_COVID_vaccine_doses_before_covid","Long_COVID_diagnosis_post_covid_indicator","COVID_hospitalization_length_of_stay","Severity_Type","age_at_covid","BMI_max_observed_or_calculated_before_covid","gender_concept_name", "ethnicity_concept_name", "race_concept_name","smoking_status","antipsychotics_before_covid_indicator","TUBERCULOSIS_before_covid_indicator","MILDLIVERDISEASE_before_covid_indicator","MODERATESEVERELIVERDISEASE_before_covid_indicator","THALASSEMIA_before_covid_indicator","RHEUMATOLOGICDISEASE_before_covid_indicator","DEMENTIA_before_covid_indicator","CONGESTIVEHEARTFAILURE_before_covid_indicator","KIDNEYDISEASE_before_covid_indicator","MALIGNANTCANCER_before_covid_indicator","DIABETESCOMPLICATED_before_covid_indicator","CEREBROVASCULARDISEASE_before_covid_indicator","PERIPHERALVASCULARDISEASE_before_covid_indicator","HEARTFAILURE_before_covid_indicator","HEMIPLEGIAORPARAPLEGIA_before_covid_indicator","PSYCHOSIS_before_covid_indicator","CORONARYARTERYDISEASE_before_covid_indicator","SYSTEMICCORTICOSTEROIDS_before_covid_indicator","DEPRESSION_before_covid_indicator","HIVINFECTION_before_covid_indicator","METASTATICSOLIDTUMORCANCERS_before_covid_indicator","CHRONICLUNGDISEASE_before_covid_indicator","MYOCARDIALINFARCTION_before_covid_indicator","DIABETESUNCOMPLICATED_before_covid_indicator","mental_health_before_covid_indicator","CARDIOMYOPATHIES_before_covid_indicator","HYPERTENSION_before_covid_indicator","TOBACCOSMOKER_before_covid_indicator","OBESITY_before_covid_indicator","SUBSTANCEABUSE_before_covid_indicator","dexamethasone_during_covid_hospitalization_indicator","tocilizumab_during_covid_hospitalization_indicator","sarilumab_during_covid_hospitalization_indicator","baricitinib_during_covid_hospitalization_indicator","tofacitinib_during_covid_hospitalization_indicator","REMDISIVIR_during_covid_hospitalization_indicator","MI","CHF","PVD","stroke","dementia","pulmonary","liver_mild","liversevere","type2_diabetes","paralysis","renal","cancer","mets","hiv","COVID_associated_hospitalization_indicator","dexamethasone_before_covid_indicator", "tocilizumab_before_covid_indicator", "sarilumab_before_covid_indicator", "baricitinib_before_covid_indicator", "tofacitinib_before_covid_indicator")

# renamed columns with long names 
    df = df.withColumnRenamed('Long_COVID_diagnosis_post_covid_indicator','Long_covid')\
           .withColumnRenamed('COVID_hospitalization_length_of_stay','Length_of_stay')\
           .withColumnRenamed('age_at_covid','Age')\
           .withColumnRenamed('gender_concept_name','Gender')\
           .withColumnRenamed('ethnicity_concept_name','Ethnicity')\
           .withColumnRenamed('race_concept_name','Race')\
           .withColumnRenamed('BMI_max_observed_or_calculated_before_covid','BMI')\
           .withColumnRenamed('TUBERCULOSIS_before_covid_indicator','Tuberculosis')\
           .withColumnRenamed('MILDLIVERDISEASE_before_covid_indicator','MILDLIVERDISEASE_before_covid')\
           .withColumnRenamed('MODERATESEVERELIVERDISEASE_before_covid_indicator','Liver_severe_moderat')\
           .withColumnRenamed('THALASSEMIA_before_covid_indicator','Thalassemia')\
           .withColumnRenamed('RHEUMATOLOGICDISEASE_before_covid_indicator','Rheumatologic_disease')\
           .withColumnRenamed('DEMENTIA_before_covid_indicator','DEMENTIA_before_covid')\
           .withColumnRenamed('CONGESTIVEHEARTFAILURE_before_covid_indicator','Congestive_heart_failure')\
           .withColumnRenamed('KIDNEYDISEASE_before_covid_indicator','Kidney_disease')\
           .withColumnRenamed('MALIGNANTCANCER_before_covid_indicator','Malignant_cancer')\
           .withColumnRenamed('DIABETESCOMPLICATED_before_covid_indicator','DMCx')\
           .withColumnRenamed('CEREBROVASCULARDISEASE_before_covid_indicator','Cerebro_vascular_disease')\
           .withColumnRenamed('PERIPHERALVASCULARDISEASE_before_covid_indicator','Peripheral_vascular_disease')\
           .withColumnRenamed('HEARTFAILURE_before_covid_indicator','Heart_failure')\
           .withColumnRenamed('HEMIPLEGIAORPARAPLEGIA_before_covid_indicator','Hemiplegia_or_paraplegia')\
           .withColumnRenamed('PSYCHOSIS_before_covid_indicator','Psychosis')\
           .withColumnRenamed('CORONARYARTERYDISEASE_before_covid_indicator','Coronary_artery_disease')\
           .withColumnRenamed('SYSTEMICCORTICOSTEROIDS_before_covid_indicator','systemic_corticosteroids')\
           .withColumnRenamed('DEPRESSION_before_covid_indicator','Depression')\
           .withColumnRenamed('METASTATICSOLIDTUMORCANCERS_before_covid_indicator','metastatic_solid_tumor_cancer')\
           .withColumnRenamed('HIVINFECTION_before_covid_indicator','HIVINFECTION')\
           .withColumnRenamed('CHRONICLUNGDISEASE_before_covid_indicator','Chronic_lung_disease')\
           .withColumnRenamed('MYOCARDIALINFARCTION_before_covid_indicator','MYOCARDIALINFARCTION')\
           .withColumnRenamed('CARDIOMYOPATHIES_before_covid_indicator','Cardiomyophaties')\
           .withColumnRenamed('HYPERTENSION_before_covid_indicator','HTN')

    # renamed Severity_types long named into small names
    df = df.withColumn('Severity_Type',regexp_replace('Severity_Type','Mild_No_ED_or_Hosp_around_COVID_index','Mild'))
    df = df.withColumn('Severity_Type',regexp_replace('Severity_Type','Mild_ED_around_COVID_index','Mild_ED'))
    df = df.withColumn('Severity_Type',regexp_replace('Severity_Type','Moderate_Hosp_around_COVID_index','Moderate'))
    df = df.withColumn('Severity_Type',regexp_replace('Severity_Type','Severe_ECMO_IMV_in_Hosp_around_COVID_index','Severe'))
    df = df.withColumn('Severity_Type',regexp_replace('Severity_Type','Death_after_COVID_index','Death')).toPandas()

    # renamed smoking_status long named into small names 
    df['smoking_status'] = df['smoking_status'].replace({'Current or Former':'Current_or_Former'})

    # renamed Ethnicity long named into small names or grouped in wide cotegory
    df['Ethnicity'] = df['Ethnicity'].replace({'Not Hispanic or Latino':'Not_Hispanic_or_Latino','Hispanic or Latino':'Hispanic_or_Latino','No matching concept':'Unknown','Other/Unknown':'Unknown','No information':'Unknown','Patient ethnicity unknown':'Unknown','Other':'Unknown' }).fillna('Unknown')

    # renamed Race long named into small names or grouped in wide cotegory

    df['Race'] = df['Race'].replace({'Black or African American':'Black_or_African_American','Black':'Black_or_African_American', 'Asian or Pacific islander':'Asian','Asian Indian':'Asian','Filipino':'Asian','Chinese':'Asian','Korean':'Asian','Vietnamese':'Asian','Japanese':'Asian','Native Hawaiian or Other Pacific Islander':'Native_Hawaiian_or_Other_Pacific_Islander','Other Pacific Islander':'Native_Hawaiian_or_Other_Pacific_Islander','No matching concept':'Unknown','Other Race':'Unknown','Other':'Unknown','No information':'Unknown','Multiple races':'Unknown','Multiple race':'Unknown','Unknown racial group':'Unknown','Hispanic':'Unknown','Polynesian':'Unknown','Refuse to answer':'Unknown','More than one race':'Unknown'}).fillna('Unknown')
    
    
    return df

@transform_pandas(
    Output(rid="ri.vector.main.execute.ed58a0ac-3ea1-454f-9c2a-4e6386652536"),
    hospitalized=Input(rid="ri.foundry.main.dataset.69ed467b-2aa0-4e55-8c57-c43fc037ee53")
)
'''
Following code for univariate, multivariate logistic regression models of COVID-19
severity(life-treatening) association with pre-existing AID, prior exposure of IS or both (AID+IS)
stratified by race. 
'''
import sys
import pandas as pd
import numpy as np
from patsy import dmatrices
import statsmodels.api as sm
import patsy
print('numpy version is:',np.__version__)
print('pandas version is:',pd.__version__)
print('statsmodel version is:',sm.__version__)
print('patsy version is:', patsy.__version__)
from pyspark.sql import functions as F
from pyspark.sql.functions import *
from pyspark.sql.functions import col, when, sum

def hospitalised_Race_AID_IS_AIDIS_model(hospitalized):
    df = hospitalized
# Model for all variants including Omicron (Comment both conditions above) 
    df = df.select("covid_diagnosis_date","Age","BMI","COVID_vaccine_doses","Long_covid","Severity","AID","immunosuppressants","TNF_inhibitor","MI","CHF","PVD","stroke","dementia","pulmonary","liver_mild","liversevere","type2_diabetes","renal","cancer","mets","hiv","Gender_FEMALE","Gender_MALE","Race_Asian","Race_Black_or_African_American","Race_Native_Hawaiian_or_Other_Pacific_Islander","Race_Unknown","Race_White","Ethnicity_Hispanic_or_Latino","Ethnicity_Not_Hispanic_or_Latino","Ethnicity_Unknown","smoking_status_Current_or_Former","smoking_status_Non_smoker",'selected_antiviral_treatment')#.toPandas() 

    df_new = df.withColumn('new_immunosuppressants',2*F.col('immunosuppressants')).drop('immunosuppressants')
    df = df_new.withColumn('AID_IS', df_new['new_immunosuppressants'] + df_new['AID'])

    # renamed Severity_types long named into small names
    df = df.withColumn("AID_IS",regexp_replace("AID_IS",'1','AID'))
    df = df.withColumn("AID_IS",regexp_replace("AID_IS",'2','IS'))
    df = df.withColumn("AID_IS",regexp_replace("AID_IS",'3','AID_immuno'))
    df = df.withColumn("AID_IS",regexp_replace("AID_IS",'0','control'))

    df = df.where(F.col('covid_diagnosis_date') <= '2022-06-30').toPandas() 

    df['cardiovascular_disease'] = df['MI'] + df['CHF'] + df['PVD'] + df['stroke'] 
    df.loc[df['cardiovascular_disease']>0, 'cardiovascular_disease'] = 1

############# Model for Race ########################

## stratified for White 
    df = df[df["Race_White"] == 1]

# stratified for Race_Black_or_African_American
#    df = df[df["Race_Black_or_African_American"] == 1]

    print('Race_yes:',df['Severity'].sum())
    print('Race_No:',df.shape[0]-df['Severity'].sum())

    print('yes:',df['Severity'].sum())
    print('No:',df.shape[0]-df['Severity'].sum())
    print('hospitalized_yes:',df['Severity'].sum())
    print('hospitalized_No:',df.shape[0]-df['Severity'].sum())

    ##### univariate model
#    l = ['control','AID','IS','AID_immuno']
#    y, X = dmatrices( 'Severity ~ C(AID_IS, levels = l)', data=df, return_type='dataframe')        
    #####################

    ### multivariate model demographic only ###
#    l = ['control','AID','IS','AID_immuno']
#    y, X = dmatrices( 'Severity ~ C(AID_IS, levels = l) + Age + BMI + Gender_MALE + Ethnicity_Hispanic_or_Latino + smoking_status_Current_or_Former', data=df, return_type='dataframe')
    ###########################################   

    ### multivariate model demographic and comorbidities ###  C(AID_IS, levels = l):cardiovascular_disease
    l = ['control','AID','IS','AID_immuno']
    y, X = dmatrices( 'Severity ~ C(AID_IS, levels = l) + Age + BMI + Gender_MALE + Ethnicity_Hispanic_or_Latino + smoking_status_Current_or_Former + cardiovascular_disease + dementia + pulmonary + liver_mild + liversevere + renal + cancer + mets + type2_diabetes + hiv', data=df, return_type='dataframe')
    #print(X.sum())

 ###########################################
    print(X.sum())
    mod = sm.Logit(y, X)
    res = mod.fit()
    print (res.summary()) 
    
    names = pd.Series(mod.data.xnames)
    names = names.str.replace("_", " ")
    originals = list(names.copy())
    coefficients = res.params.values
    # Created DataFrame of Coefficients, Odds ratio, confidence intervals, p-valus and features
    coefs = pd.DataFrame({
        'coef': ['%0.3f' % elem for elem in coefficients], 
        'odds ratio': ['%0.3f' % elem for elem in np.exp(res.params.values)], 
        'Lower CI' :  ['%0.3f' % elem for elem in np.exp(res.conf_int()[0].values)], 
        'Upper CI' :  ['%0.3f' % elem for elem in np.exp(res.conf_int()[1].values)], 
        'pvalue': res.pvalues,
        'original': originals
    })#.sort_values(by='odds ratio', ascending=False)
    return coefs

    

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.69ed467b-2aa0-4e55-8c57-c43fc037ee53"),
    cohort_final=Input(rid="ri.foundry.main.dataset.29865cb1-f9a7-4a09-a556-ca0a6f592983")
)
#cohort of hospitalization (yes/no)
import sys
import pandas as pd
import numpy as np
from sklearn.preprocessing import  StandardScaler
from pyspark.sql.functions import col, when

def hospitalized(cohort_final):
    df = cohort_final
    dff = df.withColumn("Severity",
                     when(col("Severity_Type")=='Death', 1)
                     .when(col("Severity_Type")=='Severe', 1)
                     .when(col("Severity_Type")=='Moderate', 1)
                     .otherwise(0))# #

    df = dff.select("covid_diagnosis_date","Age","BMI","COVID_vaccine_doses","Long_covid","Severity","AID","immunosuppressants","Anthracyclines", "Checkpoint_inhibitor", "Cyclophosphamide", "PK_inhibitor","Monoclonal_other", "Rituximab","Other_antineoplastic_agents", "Azathioprine", "Calcineurin_inhibitor", "IL_inhibitor","JAK_inhibitor","Mycophenol","TNF_inhibitor","Other_selective_immunosuppressants","steroids_before_covid","steroids_during_covid","Glucocorticoid","MI","CHF","PVD","stroke","paralysis","dementia","pulmonary","liver_mild","liversevere","type2_diabetes","renal","cancer","mets","hiv","Gender_FEMALE","Gender_MALE","Race_Asian","Race_Black_or_African_American","Race_Native_Hawaiian_or_Other_Pacific_Islander","Race_Unknown","Race_White","Ethnicity_Hispanic_or_Latino","Ethnicity_Not_Hispanic_or_Latino","Ethnicity_Unknown","smoking_status_Current_or_Former","smoking_status_Non_smoker",'selected_antiviral_treatment').toPandas()
    
    scaler = StandardScaler()
   # Scaling continous variables Age and BMI    
    df[['Age','BMI']] = scaler.fit_transform(df[['Age','BMI']].to_numpy())

    return df    

@transform_pandas(
    Output(rid="ri.vector.main.execute.d5f65171-681f-49e5-ba8a-c4bdfdec71ce"),
    hospitalized=Input(rid="ri.foundry.main.dataset.69ed467b-2aa0-4e55-8c57-c43fc037ee53")
)
'''
Following code for univariate, multivariate logistic regression models of COVID-19
severity(life-treatening) association with pre-existing AID, prior exposure of IS or both (AID+IS). 
stratified by gender
'''
import sys
import pandas as pd
import numpy as np
from patsy import dmatrices
import statsmodels.api as sm
import patsy
print('numpy version is:',np.__version__)
print('pandas version is:',pd.__version__)
print('statsmodel version is:',sm.__version__)
print('patsy version is:', patsy.__version__)
from pyspark.sql import functions as F
from pyspark.sql.functions import *
from pyspark.sql.functions import col, when, sum

def hospitalized_stratified_white_black_gender(hospitalized):
    df = hospitalized

# Model for all variants including Omicron (Comment both conditions above) 
    df = df.select("covid_diagnosis_date","Age","BMI","COVID_vaccine_doses","Long_covid","Severity","AID","immunosuppressants","TNF_inhibitor","MI","CHF","PVD","stroke","dementia","pulmonary","liver_mild","liversevere","type2_diabetes","renal","cancer","mets","hiv","Gender_FEMALE","Gender_MALE","Race_Asian","Race_Black_or_African_American","Race_Native_Hawaiian_or_Other_Pacific_Islander","Race_Unknown","Race_White","Ethnicity_Hispanic_or_Latino","Ethnicity_Not_Hispanic_or_Latino","Ethnicity_Unknown","smoking_status_Current_or_Former","smoking_status_Non_smoker",'selected_antiviral_treatment')#.toPandas() 

    df_new = df.withColumn('new_immunosuppressants',2*F.col('immunosuppressants')).drop('immunosuppressants')
    df = df_new.withColumn('AID_IS', df_new['new_immunosuppressants'] + df_new['AID'])

    # renamed Severity_types long named into small names
    df = df.withColumn("AID_IS",regexp_replace("AID_IS",'1','AID'))
    df = df.withColumn("AID_IS",regexp_replace("AID_IS",'2','IS'))
    df = df.withColumn("AID_IS",regexp_replace("AID_IS",'3','AID_immuno'))
    df = df.withColumn("AID_IS",regexp_replace("AID_IS",'0','control'))

    df = df.where(F.col('covid_diagnosis_date') <= '2022-06-30').toPandas() 

    df['cardiovascular_disease'] = df['MI'] + df['CHF'] + df['PVD'] + df['stroke'] 
    df.loc[df['cardiovascular_disease']>0, 'cardiovascular_disease'] = 1
    
############ Model for Gender ##########
## stratified for Gender_MALE
#    df = df[df["Gender_MALE"] == 1]

# stratified for Gender_FEMALE
    df = df[df["Gender_FEMALE"] == 1]

    print('Gender_yes:',df['Severity'].sum())
    print('Gender_No:',df.shape[0]-df['Severity'].sum())

    print('yes:',df['Severity'].sum())
    print('No:',df.shape[0]-df['Severity'].sum())
    print('hospitalization_yes:',df['Severity'].sum())
    print('hospitalization_No:',df.shape[0]-df['Severity'].sum())

    df['cardiovascular_disease'] = df['MI'] + df['CHF'] + df['PVD'] + df['stroke'] 
    df.loc[df['cardiovascular_disease']>0, 'cardiovascular_disease'] = 1

    ##### univariate model
#    l = ['control','AID','IS','AID_immuno']
#    y, X = dmatrices( 'Severity ~ C(AID_IS, levels = l)', data=df, return_type='dataframe')        
    #####################

    ### multivariate model demographic only ###
#    l = ['control','AID','IS','AID_immuno']
#    y, X = dmatrices( 'Severity ~ C(AID_IS, levels = l) + Age + BMI + Race_Asian + Race_Black_or_African_American + Race_Unknown + Ethnicity_Hispanic_or_Latino + smoking_status_Current_or_Former', data=df, return_type='dataframe')
    ###########################################   

    ### multivariate model demographic and comorbidities ###  C(AID_IS, levels = l):cardiovascular_disease
    l = ['control','AID','IS','AID_immuno']
    y, X = dmatrices( 'Severity ~ C(AID_IS, levels = l) + Age + BMI + Race_Asian + Race_Black_or_African_American + Race_Unknown + Ethnicity_Hispanic_or_Latino + smoking_status_Current_or_Former + cardiovascular_disease + dementia + pulmonary + liver_mild + liversevere + renal + cancer + mets + type2_diabetes + hiv', data=df, return_type='dataframe')
    
    ###########################################
    print(X.sum())
    mod = sm.Logit(y, X)
    res = mod.fit()
    print (res.summary()) 
    
    names = pd.Series(mod.data.xnames)
    names = names.str.replace("_", " ")
    originals = list(names.copy())
    coefficients = res.params.values
    # Created DataFrame of Coefficients, Odds ratio, confidence intervals, p-valus and features
    coefs = pd.DataFrame({
        'coef': ['%0.3f' % elem for elem in coefficients], 
        'odds ratio': ['%0.3f' % elem for elem in np.exp(res.params.values)], 
        'Lower CI' :  ['%0.3f' % elem for elem in np.exp(res.conf_int()[0].values)], 
        'Upper CI' :  ['%0.3f' % elem for elem in np.exp(res.conf_int()[1].values)], 
        'pvalue': res.pvalues,
        'original': originals
    })#.sort_values(by='odds ratio', ascending=False)
    return coefs

 
    

@transform_pandas(
    Output(rid="ri.vector.main.execute.09fb80e3-c4ff-4781-8c61-be1472c1e7fb"),
    omicron_vaccinated_hospitalized_subanalysis=Input(rid="ri.foundry.main.dataset.8cdbd276-4c48-475c-8c06-ad90f014e8a0")
)
'''
Following code for univariate, multivariate logistic regression models of COVID-19
severity(life-treatening) association with pre-existing AID, prior exposure of IS or both (AID+IS). 
included vaccinated status, antiviral treatment as a covariables
'''

import sys
import pandas as pd
import numpy as np
from patsy import dmatrices
import statsmodels.api as sm
import patsy
#import forestplot as fp
#print('numpy version is:',fp.__version__)
print('numpy version is:',np.__version__)
print('pandas version is:',pd.__version__)
print('statsmodel version is:',sm.__version__)
print('patsy version is:', patsy.__version__)
from pyspark.sql import functions as F
from pyspark.sql.functions import *
from pyspark.sql.functions import col, when, sum

def hospitalized_uni_multi_AID_immuno_vaccine(omicron_vaccinated_hospitalized_subanalysis):
    df = omicron_vaccinated_hospitalized_subanalysis

    df = df.select("covid_diagnosis_date","Age","BMI","COVID_vaccine_doses","Long_covid","Severity","AID","immunosuppressants","TNF_inhibitor","MI","CHF","PVD","stroke","paralysis","dementia","pulmonary","liver_mild","liversevere","type2_diabetes","renal","cancer","mets","hiv","Gender_FEMALE","Gender_MALE","Race_Asian","Race_Black_or_African_American","Race_Native_Hawaiian_or_Other_Pacific_Islander","Race_Unknown","Race_White","Ethnicity_Hispanic_or_Latino","Ethnicity_Not_Hispanic_or_Latino","Ethnicity_Unknown","smoking_status_Current_or_Former","smoking_status_Non_smoker",'selected_antiviral_treatment')

    df_new = df.withColumn('new_immunosuppressants',2*F.col('immunosuppressants')).drop('immunosuppressants')
    df = df_new.withColumn('AID_IS', df_new['new_immunosuppressants'] + df_new['AID'])

    # renamed Severity_types long named into small names
    df = df.withColumn("AID_IS",regexp_replace("AID_IS",'1','AID'))
    df = df.withColumn("AID_IS",regexp_replace("AID_IS",'2','IS'))
    df = df.withColumn("AID_IS",regexp_replace("AID_IS",'3','AID_immuno'))
    df = df.withColumn("AID_IS",regexp_replace("AID_IS",'0','control'))

    df = df.where(F.col('covid_diagnosis_date') <= '2022-06-30').toPandas() # or '2022-06-30','2021-12-18'

    print('lifethretening_yes:',df['Severity'].sum())
    print('lifethretening_No:',df.shape[0]-df['Severity'].sum())

    df['Age'] =( df['Age'] - df['Age'].mean() ) / df['Age'].std()
    df['BMI'] =( df['BMI'] - df['BMI'].mean() ) / df['BMI'].std()

    df['cardiovascular_disease'] = df['MI'] + df['CHF'] + df['PVD'] + df['stroke'] 
    df.loc[df['cardiovascular_disease']>0, 'cardiovascular_disease'] = 1

    ##### univariate model
#    l = ['control','AID','IS','AID_immuno']
#    y, X = dmatrices( 'Severity ~ C(AID_IS, levels = l)', data=df, return_type='dataframe')        
    #####################

    ### multivariate model demographic only ###
#    l = ['control','AID','IS','AID_immuno']
#    y, X = dmatrices( 'Severity ~ C(AID_IS, levels = l) + Age + BMI + Gender_MALE + Race_Asian + Race_Black_or_African_American + Race_Unknown + Ethnicity_Hispanic_or_Latino + smoking_status_Current_or_Former', data=df, return_type='dataframe')
    ###########################################   

    ### multivariate model demographic and comorbidities ###  C(AID_IS, levels = l):cardiovascular_disease
    l = ['control','AID','IS','AID_immuno']
    y, X = dmatrices( 'Severity ~ C(AID_IS, levels = l) + Age + BMI + Gender_MALE + Race_Asian + Race_Black_or_African_American + Race_Unknown + Ethnicity_Hispanic_or_Latino + smoking_status_Current_or_Former + cardiovascular_disease + dementia + pulmonary + liver_mild + liversevere + renal + cancer + mets + type2_diabetes + hiv + COVID_vaccine_doses + selected_antiviral_treatment', data=df, return_type='dataframe')

    X.rename(columns = {'C(AID_IS, levels=l)[T.AID]': 'AID_only ', 'C(AID_IS, levels=l)[T.IS]': 'IS_only', 'C(AID_IS, levels=l)[T.AID_immuno]':'AID_IS_only'},inplace = True) 

    ###########################################
    print(X.sum())
    mod = sm.Logit(y, X)
    res = mod.fit()
    print (res.summary()) 
    
    names = pd.Series(mod.data.xnames)
    names = names.str.replace("_", " ")
    originals = list(names.copy())
    coefficients = res.params.values
    
    print(names)
    # Created DataFrame of Coefficients, Odds ratio, confidence intervals, p-valus and features
    coefs = pd.DataFrame({
        'coef': ['%0.3f' % elem for elem in coefficients], 
        'odds ratio': ['%0.3f' % elem for elem in np.exp(res.params.values)], 
        'Lower CI' :  ['%0.3f' % elem for elem in np.exp(res.conf_int()[0].values)], 
        'Upper CI' :  ['%0.3f' % elem for elem in np.exp(res.conf_int()[1].values)], 
        'pvalue': res.pvalues,
        'original': originals
    })#.sort_values(by='odds ratio', ascending=False)
    return coefs

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.4ece3b88-6daf-47df-bd49-5b09c28060e9"),
    Patients_on_immunosupressants=Input(rid="ri.foundry.main.dataset.82472975-bf67-45df-8155-560e994e781c")
)
from pyspark.sql.functions import isnan, when, count, col, lit
from pyspark.sql import functions as F

def immunosuppressants(Patients_on_immunosupressants):
    df = Patients_on_immunosupressants
    #    Create a new variables Immunosupressants with value 1
    df = df.withColumn('immunosuppressants',lit(1))
    return df

@transform_pandas(
    Output(rid="ri.vector.main.execute.4435484f-fbe2-4c74-8fb1-3ecfa4c701dd"),
    life_thretening=Input(rid="ri.foundry.main.dataset.b200dee4-fec5-4323-88c8-e096f63f3e9f")
)
'''
Following code for univariate, multivariate logistic regression models of COVID-19
severity(life-treatening) association with pre-existing AID, prior exposure of IS or both (AID+IS)
stratified by race. 
'''
import sys
import pandas as pd
import numpy as np
from patsy import dmatrices
import statsmodels.api as sm
import patsy
print('numpy version is:',np.__version__)
print('pandas version is:',pd.__version__)
print('statsmodel version is:',sm.__version__)
print('patsy version is:', patsy.__version__)
from pyspark.sql import functions as F
from pyspark.sql.functions import *
from pyspark.sql.functions import col, when, sum

def life_threatening_Race_AID_IS_AIDIS_model(life_thretening):
    df = life_thretening

# Model for all variants including Omicron (Comment both conditions above) 
    df = df.select("covid_diagnosis_date","Age","BMI","COVID_vaccine_doses","Long_covid","Severity","AID","immunosuppressants","TNF_inhibitor","MI","CHF","PVD","stroke","dementia","pulmonary","liver_mild","liversevere","type2_diabetes","renal","cancer","mets","hiv","Gender_FEMALE","Gender_MALE","Race_Asian","Race_Black_or_African_American","Race_Native_Hawaiian_or_Other_Pacific_Islander","Race_Unknown","Race_White","Ethnicity_Hispanic_or_Latino","Ethnicity_Not_Hispanic_or_Latino","Ethnicity_Unknown","smoking_status_Current_or_Former","smoking_status_Non_smoker",'selected_antiviral_treatment')#.toPandas() 

    df_new = df.withColumn('new_immunosuppressants',2*F.col('immunosuppressants')).drop('immunosuppressants')
    df = df_new.withColumn('AID_IS', df_new['new_immunosuppressants'] + df_new['AID'])

    # renamed Severity_types long named into small names
    df = df.withColumn("AID_IS",regexp_replace("AID_IS",'1','AID'))
    df = df.withColumn("AID_IS",regexp_replace("AID_IS",'2','IS'))
    df = df.withColumn("AID_IS",regexp_replace("AID_IS",'3','AID_immuno'))
    df = df.withColumn("AID_IS",regexp_replace("AID_IS",'0','control'))

    df = df.where(F.col('covid_diagnosis_date') <= '2022-06-30').toPandas() 

    df['cardiovascular_disease'] = df['MI'] + df['CHF'] + df['PVD'] + df['stroke'] 
    df.loc[df['cardiovascular_disease']>0, 'cardiovascular_disease'] = 1

############# Model for Race ########################
#####################################################
## stratified for White 
    df = df[df["Race_White"] == 1]

# stratified for Race_Black_or_African_American
#    df = df[df["Race_Black_or_African_American"] == 1]

    print('yes:',df['Severity'].sum())
    print('No:',df.shape[0]-df['Severity'].sum())
    print('lifethretening_yes:',df['Severity'].sum())
    print('lifethretening_No:',df.shape[0]-df['Severity'].sum())

    ##### univariate model
#    l = ['control','AID','IS','AID_immuno']
#    y, X = dmatrices( 'Severity ~ C(AID_IS, levels = l)', data=df, return_type='dataframe')        
    #####################

    ### multivariate model demographic only ###
#    l = ['control','AID','IS','AID_immuno']
#    y, X = dmatrices( 'Severity ~ C(AID_IS, levels = l) + Age + BMI + Gender_MALE + Ethnicity_Hispanic_or_Latino + smoking_status_Current_or_Former', data=df, return_type='dataframe')
    ###########################################   

    ### multivariate model demographic and comorbidities ###  C(AID_IS, levels = l):cardiovascular_disease
    l = ['control','AID','IS','AID_immuno']
    y, X = dmatrices( 'Severity ~ C(AID_IS, levels = l) + Age + BMI + Gender_MALE + Ethnicity_Hispanic_or_Latino + smoking_status_Current_or_Former + cardiovascular_disease + dementia + pulmonary + liver_mild + liversevere + renal + cancer + mets + type2_diabetes + hiv', data=df, return_type='dataframe')
    #print(X.sum())

    ###########################################
    ###########################################
    print(X.sum())
    mod = sm.Logit(y, X)
    res = mod.fit()
    print (res.summary()) 
    
    names = pd.Series(mod.data.xnames)
    names = names.str.replace("_", " ")
    originals = list(names.copy())
    coefficients = res.params.values
    # Created DataFrame of Coefficients, Odds ratio, confidence intervals, p-valus and features
    coefs = pd.DataFrame({
        'coef': ['%0.3f' % elem for elem in coefficients], 
        'odds ratio': ['%0.3f' % elem for elem in np.exp(res.params.values)], 
        'Lower CI' :  ['%0.3f' % elem for elem in np.exp(res.conf_int()[0].values)], 
        'Upper CI' :  ['%0.3f' % elem for elem in np.exp(res.conf_int()[1].values)], 
        'pvalue': res.pvalues,
        'original': originals
    })#.sort_values(by='odds ratio', ascending=False)
    return coefs

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.b200dee4-fec5-4323-88c8-e096f63f3e9f"),
    cohort_final=Input(rid="ri.foundry.main.dataset.29865cb1-f9a7-4a09-a556-ca0a6f592983")
)

#cohort of life thretening (yes/no)
import sys
import pandas as pd
import numpy as np
from sklearn.preprocessing import  StandardScaler
from pyspark.sql.functions import col, when

def life_thretening(cohort_final):
    df = cohort_final

    dff = df.withColumn("Severity",
                     when(col("Severity_Type")=='Death', 1)
                     .when(col("Severity_Type")=='Severe', 1)
                     .otherwise(0))

    df = dff.select("covid_diagnosis_date","Age","BMI","COVID_vaccine_doses","Long_covid","Severity","AID","immunosuppressants","Anthracyclines", "Checkpoint_inhibitor", "Cyclophosphamide", "PK_inhibitor","Monoclonal_other", "Rituximab","Other_antineoplastic_agents", "Azathioprine", "Calcineurin_inhibitor", "IL_inhibitor","JAK_inhibitor","Mycophenol","TNF_inhibitor","Other_selective_immunosuppressants","Glucocorticoid","steroids_before_covid","steroids_during_covid","MI","CHF","PVD","stroke","paralysis","dementia","pulmonary","liver_mild","liversevere","type2_diabetes","renal","cancer","mets","hiv","Gender_FEMALE","Gender_MALE","Race_Asian","Race_Black_or_African_American","Race_Native_Hawaiian_or_Other_Pacific_Islander","Race_Unknown","Race_White","Ethnicity_Hispanic_or_Latino","Ethnicity_Not_Hispanic_or_Latino","Ethnicity_Unknown","smoking_status_Current_or_Former","smoking_status_Non_smoker",'selected_antiviral_treatment').toPandas()
   
    scaler = StandardScaler()
   # Scaling continous variables Age and BMI 
    df[['Age','BMI']] = scaler.fit_transform(df[['Age','BMI']].to_numpy())

    return df
    

@transform_pandas(
    Output(rid="ri.vector.main.execute.a7ebd242-9454-4e1e-8cc5-751ad8ad11fb"),
    omicron_vaccinated_cohort_subanalysis=Input(rid="ri.foundry.main.dataset.d6e9e805-2d2b-4e41-b503-98619e1726ee")
)
'''
Following code for univariate, multivariate logistic regression models of COVID-19
severity(life-treatening) association with pre-existing AID, prior exposure of IS or both (AID+IS). 
included vaccinated status as a covariable
'''

import sys
import pandas as pd
import numpy as np
from patsy import dmatrices
import statsmodels.api as sm
import patsy
#import forestplot as fp
#print('numpy version is:',fp.__version__)
print('numpy version is:',np.__version__)
print('pandas version is:',pd.__version__)
print('statsmodel version is:',sm.__version__)
print('patsy version is:', patsy.__version__)
from pyspark.sql import functions as F
from pyspark.sql.functions import *
from pyspark.sql.functions import col, when, sum

def lifethretening_uni_multi_AID_immuno_vaccine(omicron_vaccinated_cohort_subanalysis):
    df = omicron_vaccinated_cohort_subanalysis

    df = df.select("covid_diagnosis_date","Age","BMI","COVID_vaccine_doses","Long_covid","Severity","AID","immunosuppressants","TNF_inhibitor","MI","CHF","PVD","stroke","paralysis","dementia","pulmonary","liver_mild","liversevere","type2_diabetes","renal","cancer","mets","hiv","Gender_FEMALE","Gender_MALE","Race_Asian","Race_Black_or_African_American","Race_Native_Hawaiian_or_Other_Pacific_Islander","Race_Unknown","Race_White","Ethnicity_Hispanic_or_Latino","Ethnicity_Not_Hispanic_or_Latino","Ethnicity_Unknown","smoking_status_Current_or_Former","smoking_status_Non_smoker",'selected_antiviral_treatment')

    df_new = df.withColumn('new_immunosuppressants',2*F.col('immunosuppressants')).drop('immunosuppressants')
    df = df_new.withColumn('AID_IS', df_new['new_immunosuppressants'] + df_new['AID'])

    # renamed Severity_types long named into small names
    df = df.withColumn("AID_IS",regexp_replace("AID_IS",'1','AID'))
    df = df.withColumn("AID_IS",regexp_replace("AID_IS",'2','IS'))
    df = df.withColumn("AID_IS",regexp_replace("AID_IS",'3','AID_immuno'))
    df = df.withColumn("AID_IS",regexp_replace("AID_IS",'0','control'))

    df = df.where(F.col('covid_diagnosis_date') <= '2022-06-30').toPandas() # or '2022-06-30','2021-12-18'

    print('lifethretening_yes:',df['Severity'].sum())
    print('lifethretening_No:',df.shape[0]-df['Severity'].sum())

    df['Age'] =( df['Age'] - df['Age'].mean() ) / df['Age'].std()
    df['BMI'] =( df['BMI'] - df['BMI'].mean() ) / df['BMI'].std()

    df['cardiovascular_disease'] = df['MI'] + df['CHF'] + df['PVD'] + df['stroke'] 
    df.loc[df['cardiovascular_disease']>0, 'cardiovascular_disease'] = 1

    ##### univariate model
#    l = ['control','AID','IS','AID_immuno']
#    y, X = dmatrices( 'Severity ~ C(AID_IS, levels = l)', data=df, return_type='dataframe')        
    #####################

    ### multivariate model demographic only ###
#    l = ['control','AID','IS','AID_immuno']
#    y, X = dmatrices( 'Severity ~ C(AID_IS, levels = l) + Age + BMI + Gender_MALE + Race_Asian + Race_Black_or_African_American + Race_Unknown + Ethnicity_Hispanic_or_Latino + smoking_status_Current_or_Former', data=df, return_type='dataframe')
    ###########################################   

    ### multivariate model demographic and comorbidities ###  C(AID_IS, levels = l):cardiovascular_disease
    l = ['control','AID','IS','AID_immuno']
    y, X = dmatrices( 'Severity ~ C(AID_IS, levels = l) + Age + BMI + Gender_MALE + Race_Asian + Race_Black_or_African_American + Race_Unknown + Ethnicity_Hispanic_or_Latino + smoking_status_Current_or_Former + cardiovascular_disease + dementia + pulmonary + liver_mild + liversevere + renal + cancer + mets + type2_diabetes + hiv + COVID_vaccine_doses + selected_antiviral_treatment', data=df, return_type='dataframe')

    X.rename(columns = {'C(AID_IS, levels=l)[T.AID]': 'AID_only ', 'C(AID_IS, levels=l)[T.IS]': 'IS_only', 'C(AID_IS, levels=l)[T.AID_immuno]':'AID_IS_only'},inplace = True) 

    ###########################################
    print(X.sum())
    mod = sm.Logit(y, X)
    res = mod.fit()
    print (res.summary()) 
    
    names = pd.Series(mod.data.xnames)
    names = names.str.replace("_", " ")
    originals = list(names.copy())
    coefficients = res.params.values
    
    print(names)
    # Created DataFrame of Coefficients, Odds ratio, confidence intervals, p-valus and features
    coefs = pd.DataFrame({
        'coef': ['%0.3f' % elem for elem in coefficients], 
        'odds ratio': ['%0.3f' % elem for elem in np.exp(res.params.values)], 
        'Lower CI' :  ['%0.3f' % elem for elem in np.exp(res.conf_int()[0].values)], 
        'Upper CI' :  ['%0.3f' % elem for elem in np.exp(res.conf_int()[1].values)], 
        'pvalue': res.pvalues,
        'original': originals
    })#.sort_values(by='odds ratio', ascending=False)
    return coefs

@transform_pandas(
    Output(rid="ri.vector.main.execute.f2cb45f7-3a5b-463b-8b8d-a74b0d80fb8c"),
    life_thretening=Input(rid="ri.foundry.main.dataset.b200dee4-fec5-4323-88c8-e096f63f3e9f")
)
'''
Following code for univariate, multivariate logistic regression models of COVID-19
severity(life-treatening) association with pre-existing AID, prior exposure of IS or both (AID+IS). 
results are in table 2 & eTable 3a
'''

import sys
import pandas as pd
import numpy as np
from patsy import dmatrices
import statsmodels.api as sm
import patsy
#import forestplot as fp
#print('numpy version is:',fp.__version__)
print('numpy version is:',np.__version__)
print('pandas version is:',pd.__version__)
print('statsmodel version is:',sm.__version__)
print('patsy version is:', patsy.__version__)
from pyspark.sql import functions as F
from pyspark.sql.functions import *
from pyspark.sql.functions import col, when, sum

def lifethretening_uni_multi_AID_immunosuppressants(life_thretening):
    df = life_thretening

    df = df.select("covid_diagnosis_date","Age","BMI","COVID_vaccine_doses","Long_covid","Severity","AID","immunosuppressants","TNF_inhibitor","MI","CHF","PVD","stroke","dementia","pulmonary","liver_mild","liversevere","type2_diabetes","renal","cancer","mets","hiv","Gender_FEMALE","Gender_MALE","Race_Asian","Race_Black_or_African_American","Race_Native_Hawaiian_or_Other_Pacific_Islander","Race_Unknown","Race_White","Ethnicity_Hispanic_or_Latino","Ethnicity_Not_Hispanic_or_Latino","Ethnicity_Unknown","smoking_status_Current_or_Former","smoking_status_Non_smoker",'selected_antiviral_treatment')

    df_new = df.withColumn('new_immunosuppressants',2*F.col('immunosuppressants')).drop('immunosuppressants')
    df = df_new.withColumn('AID_IS', df_new['new_immunosuppressants'] + df_new['AID'])

    # renamed Severity_types long named into small names
    df = df.withColumn("AID_IS",regexp_replace("AID_IS",'1','AID'))
    df = df.withColumn("AID_IS",regexp_replace("AID_IS",'2','IS'))
    df = df.withColumn("AID_IS",regexp_replace("AID_IS",'3','AID_immuno'))
    df = df.withColumn("AID_IS",regexp_replace("AID_IS",'0','control'))

    df = df.where(F.col('covid_diagnosis_date') <= '2022-06-30').toPandas() # or '2022-06-30','2021-12-18'

    print('lifethretening_yes:',df['Severity'].sum())
    print('lifethretening_No:',df.shape[0]-df['Severity'].sum())

    df['cardiovascular_disease'] = df['MI'] + df['CHF'] + df['PVD'] + df['stroke'] 
    df.loc[df['cardiovascular_disease']>0, 'cardiovascular_disease'] = 1

    ##### univariate model
#    l = ['control','AID','IS','AID_immuno']
#    y, X = dmatrices( 'Severity ~ C(AID_IS, levels = l)', data=df, return_type='dataframe')        
    #####################

    ### multivariate model demographic only ###
    l = ['control','AID','IS','AID_immuno']
    y, X = dmatrices( 'Severity ~ C(AID_IS, levels = l) + Age + BMI + Gender_MALE + Race_Asian + Race_Black_or_African_American + Race_Unknown + Ethnicity_Hispanic_or_Latino + smoking_status_Current_or_Former', data=df, return_type='dataframe')
    ###########################################   

    ### multivariate model demographic and comorbidities ###  C(AID_IS, levels = l):cardiovascular_disease
#    l = ['control','AID','IS','AID_immuno']
#    y, X = dmatrices( 'Severity ~ C(AID_IS, levels = l) + Age + BMI + Gender_MALE + Race_Asian + Race_Black_or_African_American + Race_Unknown + Ethnicity_Hispanic_or_Latino + smoking_status_Current_or_Former + cardiovascular_disease + dementia + pulmonary + liver_mild + liversevere + renal + cancer + mets + type2_diabetes + hiv', data=df, return_type='dataframe')

    X.rename(columns = {'C(AID_IS, levels=l)[T.AID]': 'AID_only ', 'C(AID_IS, levels=l)[T.IS]': 'IS_only', 'C(AID_IS, levels=l)[T.AID_immuno]':'AID_IS_only'},inplace = True) 

    ###########################################
    print(X.sum())
    mod = sm.Logit(y, X)
    res = mod.fit()
    print (res.summary()) 
    
    names = pd.Series(mod.data.xnames)
    names = names.str.replace("_", " ")
    originals = list(names.copy())
    coefficients = res.params.values
    
    print(names)
    # Created DataFrame of Coefficients, Odds ratio, confidence intervals, p-valus and features
    coefs = pd.DataFrame({
        'coef': ['%0.3f' % elem for elem in coefficients], 
        'odds ratio': ['%0.3f' % elem for elem in np.exp(res.params.values)], 
        'Lower CI' :  ['%0.3f' % elem for elem in np.exp(res.conf_int()[0].values)], 
        'Upper CI' :  ['%0.3f' % elem for elem in np.exp(res.conf_int()[1].values)], 
        'pvalue': res.pvalues,
        'original': originals
    })#.sort_values(by='odds ratio', ascending=False)
    return coefs

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.d6e9e805-2d2b-4e41-b503-98619e1726ee"),
    cohort_final=Input(rid="ri.foundry.main.dataset.29865cb1-f9a7-4a09-a556-ca0a6f592983")
)

from pyspark.sql import functions as F
from pyspark.sql.functions import col, when

def omicron_vaccinated_cohort_subanalysis(cohort_final):
    df = cohort_final

    dff = df.withColumn("Severity",
                    when(col("Severity_Type")=='Death', 1)
                    .when(col("Severity_Type")=='Severe', 1)
                    .otherwise(0))

    # define a list of data partner id which is included based on Hythem (shared list of data partners)
    l = ['770', '439', '198', '793', '124', '726', '507', '183'] 
    # filter out records by data prtner ids by list l
    data = dff.filter(dff.data_partner_id.isin(l))
    df_data = data.where(data.covid_diagnosis_date >= '2021-12-23')
    return df_data

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.8cdbd276-4c48-475c-8c06-ad90f014e8a0"),
    cohort_final=Input(rid="ri.foundry.main.dataset.29865cb1-f9a7-4a09-a556-ca0a6f592983")
)

from pyspark.sql import functions as F
from pyspark.sql.functions import col, when

def omicron_vaccinated_hospitalized_subanalysis(cohort_final):
    df = cohort_final

    dff = df.withColumn("Severity",
                     when(col("Severity_Type")=='Death', 1)
                     .when(col("Severity_Type")=='Severe', 1)
                     .when(col("Severity_Type")=='Moderate', 1)
                     .otherwise(0))# 

    # define a list of data partner id which is excluded based on Emily reco (naughty list)
    l = ['770', '439', '198', '793', '124', '726', '507', '183'] 
    # filter out records by data prtner ids by list l
    data = dff.filter(dff.data_partner_id.isin(l))
    df_data = data.where(data.covid_diagnosis_date >= '2021-12-23')
    return df_data

