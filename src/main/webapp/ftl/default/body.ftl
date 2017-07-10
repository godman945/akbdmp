<#if personsList?exists>
            <tr>
            <#list personsList as person>
               <tr>
	               <td>${person.firstName}</td>
	               <td>${person.lastName}</td>
	           </tr>
               </#list>
				
            </tr>
             </#if>