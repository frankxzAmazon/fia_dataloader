using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;

namespace DataLoaderOptions.MicrosoftExchange
{
    public partial class Username_Password : Form
    {
        public Username_Password()
        {
            InitializeComponent();
            logIn.DialogResult = DialogResult.OK;
            cancel.DialogResult = DialogResult.Cancel;
            cancel.Click += (s, e) => 
            {
                userName.Text = "Cancel";
                password.Text = "Cancel";
            };
            AcceptButton = logIn;
            CancelButton = cancel;
        }

        public string[] Login()
        {
            ShowDialog();
            Close();
            return new string[2] { userName.Text, password.Text };
        }
    }
}
