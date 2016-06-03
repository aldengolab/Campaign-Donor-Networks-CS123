import pandas as pd
df = pd.read_csv('../data/full-datset-corporate.csv', names = ['donor_name','organization','parent','recipient','party','seat','result','month','year','amount'])
df2 = pd.read_csv('../data/sample_individual_donations.csv', names = ['donor_name','organization','parent','recipient','party','seat','result','month','year','amount'])
###subset data into republican, democrat, and other results
republican = df[df['party'] == 'R']
democrat = df[df['party'] == 'D'] 
notr = df['party'] != 'R'
notd = df['party'] != 'D'
other = df[notr]
other = df[notd]

##### This is loaded from the individual donors dataset:
republican_indv = df2[df2['party'] == 'R']
democrat_indv = df2[df2['party'] == 'D'] 
notr_indv = df2['party'] != 'R'
notd_indv = df2['party'] != 'D'
other_indv = df2[notr]
other = df2[notd]



###### print number of republican donations, democrat donations, and other donations
# print "the number of republican corporate donations is ", len(republican), "\n"
# print "the number of democrat corporate donations is ", len(democrat), "\n"
# print "the number of other corporate donations is ", len(other), "\n"
results = democrat.groupby('organization').size().to_frame()
results.columns = ['democrat_count']
results['republican_count'] = republican.groupby('organization').size().to_frame()
results['other_count'] = other.groupby('organization').size().to_frame()
results['total_times_donated'] = results['republican_count'] + results['democrat_count'] + results['other_count']
results['republican_perc'] = results['republican_count'] / results['total_times_donated']
results['democrat_perc'] = results['democrat_count'] / results['total_times_donated']
results['other_perc'] = results['other_count'] / results['total_times_donated']
results['republican_dollars'] = republican.groupby('organization')['amount'].sum().to_frame()
results['democrat_dollars'] = democrat.groupby('organization')['amount'].sum().to_frame()
results['other_dollars'] = other.groupby('organization')['amount'].sum().to_frame()
results['total_dollars'] = results['republican_dollars'] + results['democrat_dollars'] + results['other_dollars']
results['republican_dollar_perc'] = results['republican_dollars'] / results['total_dollars']
results['democrat_dollar_perc'] = results['democrat_dollars'] / results['total_dollars']
results['other_dollar_perc'] = results['other_dollars'] / results['total_dollars']
results = results.fillna(0)





print("The top 10 Corporate Donors by Times Donated Are:")
print(results.sort_values('total_times_donated', ascending = False)['total_times_donated'].head(10))
print("\n")
print("The top 10 Corporate Donors by Dollars Donated Are: ")
print(results.sort_values('total_dollars', ascending = False)['total_dollars'].head(10))
print("\n")
print("The top 10 Democrat Donors by Dollars Donated : ")
print(results.sort_values('democrat_dollars', ascending = False)['democrat_dollars'].head(10))
print("\n")
print("The top 10 Republican Donors by Number of Dollars Donated : ")
print(results.sort_values('republican_dollars', ascending = False)['republican_dollars'].head(10))
print("\n")
print("The top 10 Other Donors by Number of Dollars Donated : ")
print(results.sort_values('other_dollars', ascending = False)['other_dollars'].head(10))
print("\n")
print("The top 10 Democrat Donors by Percentage Share of Dollars Donated : ")
print(results.sort_values('democrat_dollar_perc', ascending = False)['democrat_dollar_perc'].head(10))
print("\n")
print("The top 10 Republican Donors by Percentage Share of Dollars Donated : ")
print(results.sort_values('republican_dollar_perc', ascending = False)['republican_dollar_perc'].head(10))
print("\n")
print("The top 10 Other Donors by Percentage Share of Dollars Donated : ")
print(results.sort_values('other_dollar_perc', ascending = False)['other_dollar_perc'].head(10))
print("\n")

###### Group By recipient 
recipient_results = democrat.groupby('recipient').size().to_frame()
recipient_results.columns = ['donations_count']






















# results.columns = ['Democrat Counts']













# results = df[df['party'] == 'D'].groupby('organization').size().to_frame()
# results = df[df['party'] == 'R'].groupby('organization').size().to_frame()
# results = df[df['party'] != 'R'].groupby('organization').size().to_frame()



