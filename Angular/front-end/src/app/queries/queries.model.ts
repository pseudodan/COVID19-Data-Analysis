/* *** Query Model ***
*  Model holds the definition for every query. This model is shared by multiple query components.
*   - name: Name of the query.
*   - description: Description of the selected query.
*/
export class Query
{
  public name: string;
  public description: string;

  constructor(name: string, description: string)
  {
    this.name = name;
    this.description = description;
  }
}
