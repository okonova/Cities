Vietnē mockaroo izveidoti 2 datu faili: 
1) csv fails states.csv
2) json fails abbrev.json
csv fails nolasīts un pārveidots par spark DF
json fails atvērts kā DF
abi DF apvienoti ar inner join; vienojošā kolonna ir "city"
atmesti dublikāti
izveidots jauns DF, kas apvieno kolonnas no abiem DF
jaunais DF saglabāts kā csv fails. Izmēģinājos saglabāt kā jaunas datubāzes tabulu, bet neizdevās tikt galā ar sparka savienojumu ar datubāzi. 
izveidots artifakts
projekts ievietots githubā. 
