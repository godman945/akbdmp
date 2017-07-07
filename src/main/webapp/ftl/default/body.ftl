<#if personsList?exists>
            <tr>
            <#list personsList as person>
               <tr>
               <td>${person.firstName}</td>
               <td>${person.lastName}</td>
               </#list>
				</tr>
            </tr>
             </#if>